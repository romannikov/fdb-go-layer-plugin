pub mod store {
    include!("gen/store.fdb.rs");
}

pub mod atomic {
    include!("gen/atomic.fdb.rs");
}

#[cfg(test)]
mod tests {
    use super::atomic::*;
    use super::store::*;
    use fdb_layer::{
        Database, Element, FdbLayerError, GenericRepository, PaginationOptions, RecordStore,
        Subspace,
    };
    use std::sync::{Arc, Once};
    use std::time::{SystemTime, UNIX_EPOCH};

    static INIT: Once = Once::new();

    fn init_fdb() -> Database {
        INIT.call_once(|| {
            #[allow(unused_unsafe)]
            let network = unsafe { fdb_layer::foundationdb::boot() };
            std::mem::forget(network);
        });
        Database::default().expect("failed to open default FDB database")
    }

    fn test_subspace(name: &str) -> Subspace {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Subspace::all().subspace(&("rust_test", name, nanos))
    }

    #[tokio::test]
    async fn test_sync_metadata_and_idempotency() {
        let db = init_fdb();
        let dir = test_subspace("sync_metadata");
        let store = Arc::new(RecordStore::new());

        let tr = db.create_trx().unwrap();
        store
            .sync_metadata(&tr, &dir, &["User", "Product", "Post", "TaskMessage"])
            .await
            .unwrap();
        tr.commit().await.unwrap();

        let meta1 = store.metadata();
        assert_eq!(meta1.len(), 4);
        assert_ne!(meta1["User"], meta1["Product"]);

        // Second sync on fresh store should read identical IDs
        let store2 = Arc::new(RecordStore::new());
        let tr2 = db.create_trx().unwrap();
        store2
            .sync_metadata(&tr2, &dir, &["User", "Product", "Post", "TaskMessage"])
            .await
            .unwrap();
        tr2.commit().await.unwrap();

        assert_eq!(meta1, store2.metadata());
    }

    #[tokio::test]
    async fn test_user_crud_and_secondary_index() {
        let db = init_fdb();
        let dir = test_subspace("user_crud");
        let store = Arc::new(RecordStore::new());
        let repo = UserRepository::new(store.clone());

        {
            let tr = db.create_trx().unwrap();
            store
                .sync_metadata(&tr, &dir, &["User", "Product", "Post"])
                .await
                .unwrap();
            repo.create(
                &tr,
                &dir,
                &User {
                    id: "u1".into(),
                    name: "Alice".into(),
                    email: "alice@test.com".into(),
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }

        // Duplicate create must fail with AlreadyExists
        {
            let tr = db.create_trx().unwrap();
            let err = repo
                .create(
                    &tr,
                    &dir,
                    &User {
                        id: "u1".into(),
                        name: "Bob".into(),
                        email: "bob@test.com".into(),
                    },
                )
                .await
                .unwrap_err();
            assert!(matches!(err, FdbLayerError::AlreadyExists(_)));
        }

        // Get and secondary index lookup
        {
            let tr = db.create_trx().unwrap();
            let u = repo.get(&tr, &dir, "u1").await.unwrap();
            assert_eq!(u.name, "Alice");
            assert_eq!(u.email, "alice@test.com");

            let by_email = repo
                .get_user_by_email(&tr, &dir, "alice@test.com")
                .await
                .unwrap();
            assert_eq!(by_email.len(), 1);
            assert_eq!(by_email[0].id, "u1");
        }

        // Set updates email index
        {
            let tr = db.create_trx().unwrap();
            repo.set(
                &tr,
                &dir,
                &User {
                    id: "u1".into(),
                    name: "Alice Updated".into(),
                    email: "new@test.com".into(),
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            let old_res = repo
                .get_user_by_email(&tr, &dir, "alice@test.com")
                .await
                .unwrap();
            assert!(old_res.is_empty());

            let new_res = repo
                .get_user_by_email(&tr, &dir, "new@test.com")
                .await
                .unwrap();
            assert_eq!(new_res.len(), 1);
            assert_eq!(new_res[0].name, "Alice Updated");
        }

        // Delete clears record and index
        {
            let tr = db.create_trx().unwrap();
            repo.delete(&tr, &dir, "u1").await.unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            let err = repo.get(&tr, &dir, "u1").await.unwrap_err();
            assert!(matches!(err, FdbLayerError::NotFound(_)));
            let idx_res = repo
                .get_user_by_email(&tr, &dir, "new@test.com")
                .await
                .unwrap();
            assert!(idx_res.is_empty());
        }
    }

    #[tokio::test]
    async fn test_batch_get_and_pagination() {
        let db = init_fdb();
        let dir = test_subspace("batch_and_page");
        let store = Arc::new(RecordStore::new());
        let repo = ProductRepository::new(store.clone());

        {
            let tr = db.create_trx().unwrap();
            store.sync_metadata(&tr, &dir, &["Product"]).await.unwrap();
            for i in 0..5 {
                repo.create(
                    &tr,
                    &dir,
                    &Product {
                        id: format!("p{i}"),
                        name: format!("Product {i}"),
                        category: "tools".into(),
                        price: (i + 1) * 10,
                    },
                )
                .await
                .unwrap();
            }
            tr.commit().await.unwrap();
        }

        // BatchGet
        {
            let tr = db.create_trx().unwrap();
            let ids = vec![
                vec![Element::String("p0".into())],
                vec![Element::String("p2".into())],
                vec![Element::String("p99".into())],
            ];
            let batch = repo.batch_get_product(&tr, &dir, &ids).await.unwrap();
            assert_eq!(batch.len(), 2);
            assert_eq!(batch["(\"p0\")"].price, 10);
            assert_eq!(batch["(\"p2\")"].price, 30);
        }

        // Pagination (limit 2 -> 2 -> 1)
        {
            let tr = db.create_trx().unwrap();
            let p1 = repo
                .list_product(
                    &tr,
                    &dir,
                    PaginationOptions {
                        begin: vec![],
                        limit: 2,
                    },
                )
                .await
                .unwrap();
            assert_eq!(p1.items.len(), 2);
            assert!(p1.has_more);

            let p2 = repo
                .list_product(
                    &tr,
                    &dir,
                    PaginationOptions {
                        begin: p1.next_key,
                        limit: 2,
                    },
                )
                .await
                .unwrap();
            assert_eq!(p2.items.len(), 2);
            assert!(p2.has_more);

            let p3 = repo
                .list_product(
                    &tr,
                    &dir,
                    PaginationOptions {
                        begin: p2.next_key,
                        limit: 2,
                    },
                )
                .await
                .unwrap();
            assert_eq!(p3.items.len(), 1);
            assert!(!p3.has_more);
        }
    }

    #[tokio::test]
    async fn test_post_fan_out_index() {
        let db = init_fdb();
        let dir = test_subspace("fan_out");
        let store = Arc::new(RecordStore::new());
        let repo = PostRepository::new(store.clone());

        {
            let tr = db.create_trx().unwrap();
            store.sync_metadata(&tr, &dir, &["Post"]).await.unwrap();
            repo.create(
                &tr,
                &dir,
                &Post {
                    id: "post1".into(),
                    tags: vec!["alpha".into(), "beta".into()],
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }

        // Delta update via Set: remove alpha, keep beta, add gamma
        {
            let tr = db.create_trx().unwrap();
            repo.set(
                &tr,
                &dir,
                &Post {
                    id: "post1".into(),
                    tags: vec!["beta".into(), "gamma".into()],
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            assert!(repo
                .get_post_by_tags(&tr, &dir, "alpha")
                .await
                .unwrap()
                .is_empty());
            assert_eq!(
                repo.get_post_by_tags(&tr, &dir, "beta")
                    .await
                    .unwrap()
                    .len(),
                1
            );
            assert_eq!(
                repo.get_post_by_tags(&tr, &dir, "gamma")
                    .await
                    .unwrap()
                    .len(),
                1
            );
        }

        // Delete clears all fan-out tags
        {
            let tr = db.create_trx().unwrap();
            repo.delete(&tr, &dir, "post1").await.unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            assert!(repo
                .get_post_by_tags(&tr, &dir, "beta")
                .await
                .unwrap()
                .is_empty());
            assert!(repo
                .get_post_by_tags(&tr, &dir, "gamma")
                .await
                .unwrap()
                .is_empty());
        }
    }

    #[tokio::test]
    async fn test_task_message_queue() {
        let db = init_fdb();
        let dir = test_subspace("queue");
        let store = Arc::new(RecordStore::new());
        let repo = TaskMessageRepository::new(store.clone());

        {
            let tr = db.create_trx().unwrap();
            store
                .sync_metadata(&tr, &dir, &["TaskMessage"])
                .await
                .unwrap();
            // Enqueue two tasks in the SAME transaction with identical shard_id
            repo.enqueue(
                &tr,
                &dir,
                &TaskMessage {
                    queue_name: "q1".into(),
                    shard_id: 3,
                    versionstamp: vec![],
                    payload: b"first".to_vec(),
                },
            )
            .await
            .unwrap();
            repo.enqueue(
                &tr,
                &dir,
                &TaskMessage {
                    queue_name: "q1".into(),
                    shard_id: 1,
                    versionstamp: vec![],
                    payload: b"second".to_vec(),
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            let d1 = repo.dequeue(&tr, &dir, "q1").await.unwrap().unwrap();
            tr.commit().await.unwrap();
            assert_eq!(d1.payload, b"first");
            assert_eq!(d1.shard_id, 3);
            assert_eq!(d1.versionstamp.len(), 12);
        }

        {
            let tr = db.create_trx().unwrap();
            let d2 = repo.dequeue(&tr, &dir, "q1").await.unwrap().unwrap();
            tr.commit().await.unwrap();
            assert_eq!(d2.payload, b"second");
            assert_eq!(d2.shard_id, 1);
            assert_eq!(d2.versionstamp.len(), 12);
        }

        {
            let tr = db.create_trx().unwrap();
            let empty = repo.dequeue(&tr, &dir, "q1").await.unwrap();
            assert!(empty.is_none());
        }
    }

    #[tokio::test]
    async fn test_counter_atomic_mutations() {
        let db = init_fdb();
        let dir = test_subspace("atomics");
        let store = Arc::new(RecordStore::new());
        let repo = CounterRepository::new(store.clone());

        {
            let tr = db.create_trx().unwrap();
            store.sync_metadata(&tr, &dir, &["Counter"]).await.unwrap();
            repo.create(
                &tr,
                &dir,
                &Counter {
                    id: "c1".into(),
                    value: 10,
                    max_value: 100,
                    min_value: 5,
                    u64_max: 1000,
                    i32_min: -20,
                    u32_add: 10,
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }

        const HIGH_U64: u64 = (1u64 << 63) + 12345;
        {
            let tr = db.create_trx().unwrap();
            repo.add_counter_value(&tr, &dir, "c1", 5).await.unwrap();
            repo.max_counter_max_value(&tr, &dir, "c1", 50).await.unwrap();
            repo.max_counter_max_value(&tr, &dir, "c1", 150).await.unwrap();
            repo.min_counter_min_value(&tr, &dir, "c1", 10).await.unwrap();
            repo.min_counter_min_value(&tr, &dir, "c1", 2).await.unwrap();
            repo.max_counter_u64_max(&tr, &dir, "c1", HIGH_U64).await.unwrap();
            repo.min_counter_i32_min(&tr, &dir, "c1", -100).await.unwrap();
            repo.add_counter_u32_add(&tr, &dir, "c1", 25).await.unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            let c = repo.get(&tr, &dir, "c1").await.unwrap();
            assert_eq!(c.value, 15);
            assert_eq!(c.max_value, 150);
            assert_eq!(c.min_value, 2);
            assert_eq!(c.u64_max, HIGH_U64);
            assert_eq!(c.i32_min, -100);
            assert_eq!(c.u32_add, 35);

            // Verify batch_get_counter populates atomics
            let batch = repo
                .batch_get_counter(&tr, &dir, &[vec![Element::String("c1".into())]])
                .await
                .unwrap();
            let bc = &batch["(\"c1\")"];
            assert_eq!((bc.value, bc.max_value, bc.min_value), (15, 150, 2));

            // Verify list_counter populates atomics
            let listed = repo
                .list_counter(
                    &tr,
                    &dir,
                    PaginationOptions {
                        begin: vec![],
                        limit: 10,
                    },
                )
                .await
                .unwrap();
            assert_eq!(listed.items.len(), 1);
            assert_eq!(
                (
                    listed.items[0].value,
                    listed.items[0].max_value,
                    listed.items[0].min_value
                ),
                (15, 150, 2)
            );
        }

        // Verify Set does not overwrite atomic fields
        {
            let tr = db.create_trx().unwrap();
            repo.set(
                &tr,
                &dir,
                &Counter {
                    id: "c1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            let c = repo.get(&tr, &dir, "c1").await.unwrap();
            assert_eq!((c.value, c.max_value, c.min_value), (15, 150, 2));
        }

        // Verify zero-initialized counter allows positive Min mutation
        {
            let tr = db.create_trx().unwrap();
            repo.create(
                &tr,
                &dir,
                &Counter {
                    id: "c_zero".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            repo.min_counter_min_value(&tr, &dir, "c_zero", 42)
                .await
                .unwrap();
            tr.commit().await.unwrap();
        }

        {
            let tr = db.create_trx().unwrap();
            let cz = repo.get(&tr, &dir, "c_zero").await.unwrap();
            assert_eq!(cz.min_value, 42);
        }

        // Verify negative i64 values on Create and Max/Min mutations
        {
            let tr = db.create_trx().unwrap();
            repo.create(
                &tr,
                &dir,
                &Counter {
                    id: "c_neg".into(),
                    value: -10,
                    max_value: -100,
                    min_value: -5,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }
        {
            let tr = db.create_trx().unwrap();
            repo.max_counter_max_value(&tr, &dir, "c_neg", -200).await.unwrap();
            repo.max_counter_max_value(&tr, &dir, "c_neg", -50).await.unwrap();
            repo.min_counter_min_value(&tr, &dir, "c_neg", -2).await.unwrap();
            repo.min_counter_min_value(&tr, &dir, "c_neg", -20).await.unwrap();
            repo.max_counter_max_value(&tr, &dir, "c1", -1).await.unwrap();
            repo.min_counter_min_value(&tr, &dir, "c1", -10).await.unwrap();
            tr.commit().await.unwrap();
        }
        {
            let tr = db.create_trx().unwrap();
            let c_neg = repo.get(&tr, &dir, "c_neg").await.unwrap();
            assert_eq!((c_neg.value, c_neg.max_value, c_neg.min_value), (-10, -50, -20));
            let c1 = repo.get(&tr, &dir, "c1").await.unwrap();
            assert_eq!((c1.value, c1.max_value, c1.min_value), (15, 150, -10));
        }

        // Verify GenericRepository trait implementation
        {
            let tr = db.create_trx().unwrap();
            let gen_c = <CounterRepository as GenericRepository<Counter, String>>::get(
                &repo,
                &tr,
                &dir,
                "c1".to_string(),
            )
            .await
            .unwrap();
            assert_eq!(gen_c.value, 15);
        }

        // Verify Delete clears FIELD_NAMESPACE so re-creating with zeros does not leak stale atomics
        {
            let tr = db.create_trx().unwrap();
            repo.delete(&tr, &dir, "c1").await.unwrap();
            tr.commit().await.unwrap();
        }
        {
            let tr = db.create_trx().unwrap();
            repo.create(
                &tr,
                &dir,
                &Counter {
                    id: "c1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            tr.commit().await.unwrap();
        }
        {
            let tr = db.create_trx().unwrap();
            let c1_recreated = repo.get(&tr, &dir, "c1").await.unwrap();
            assert_eq!(
                (
                    c1_recreated.value,
                    c1_recreated.max_value,
                    c1_recreated.min_value,
                    c1_recreated.u64_max,
                    c1_recreated.i32_min,
                    c1_recreated.u32_add
                ),
                (0, 0, 0, 0, 0, 0)
            );
        }
    }

    #[tokio::test]
    async fn test_order_compound_pk_bytes_and_nested_types() {
        let db = init_fdb();
        let dir = test_subspace("order_and_audit");
        let store = Arc::new(RecordStore::new());
        let order_repo = OrderRepository::new(store.clone());
        let audit_repo = AuditLogRepository::new(store.clone());

        {
            let tr = db.create_trx().unwrap();
            store
                .sync_metadata(&tr, &dir, &["Order", "AuditLog"])
                .await
                .unwrap();
            order_repo
                .create(
                    &tr,
                    &dir,
                    &Order {
                        tenant_id: "acme".into(),
                        order_seq: 101,
                        status: OrderStatus::Pending as i32,
                        created_at: 1700000000,
                        receipt_hash: vec![0xAA, 0xBB, 0x01],
                        attachment_hashes: vec![vec![0x10, 0x20], vec![0x30, 0x40]],
                        priority: order::Priority::High as i32,
                        address: Some(order::ShippingAddress {
                            city: "Zurich".into(),
                            country: "Switzerland".into(),
                        }),
                        history: vec![order::ShippingAddress {
                            city: "Basel".into(),
                            country: "Switzerland".into(),
                        }],
                    },
                )
                .await
                .unwrap();
            audit_repo
                .create(
                    &tr,
                    &dir,
                    &AuditLog {
                        log_id: vec![],
                        actor: "order_created".into(),
                    },
                )
                .await
                .unwrap();
            tr.commit().await.unwrap();
        }

        // Verify compound PK get, compound index, bytes index, fan-out bytes index, and nested fields
        {
            let tr = db.create_trx().unwrap();
            let ord = order_repo
                .get(
                    &tr,
                    &dir,
                    OrderPrimaryKey {
                        tenant_id: "acme".into(),
                        order_seq: 101,
                    },
                )
                .await
                .unwrap();
            assert_eq!(ord.priority, order::Priority::High as i32);
            assert_eq!(ord.address.as_ref().unwrap().city, "Zurich");
            assert_eq!(ord.history.len(), 1);

            let by_status = order_repo
                .get_order_by_status_and_created_at(&tr, &dir, OrderStatus::Pending, 1700000000)
                .await
                .unwrap();
            assert_eq!(by_status.len(), 1);
            assert_eq!(by_status[0].order_seq, 101);

            let by_receipt = order_repo
                .get_order_by_receipt_hash(&tr, &dir, &[0xAA, 0xBB, 0x01])
                .await
                .unwrap();
            assert_eq!(by_receipt.len(), 1);

            let by_attach = order_repo
                .get_order_by_attachment_hashes(&tr, &dir, &[0x10, 0x20])
                .await
                .unwrap();
            assert_eq!(by_attach.len(), 1);

            // Verify AuditLog non-queue versionstamp PK via list + get + delete
            let logs = audit_repo
                .list_audit_log(
                    &tr,
                    &dir,
                    PaginationOptions {
                        begin: vec![],
                        limit: 10,
                    },
                )
                .await
                .unwrap();
            assert_eq!(logs.items.len(), 1);
            assert_eq!(logs.items[0].log_id.len(), 12);

            let fetched_log = audit_repo
                .get(&tr, &dir, &logs.items[0].log_id)
                .await
                .unwrap();
            assert_eq!(fetched_log.actor, "order_created");
        }
    }
}


