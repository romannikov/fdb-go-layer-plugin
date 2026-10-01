use fdb_layer::{Database, Element, RecordStore, Subspace};
use fdb_layer_tests::atomic::CounterRepository;
use fdb_layer_tests::store::{
    Post, PostRepository, Product, ProductRepository, TaskMessage, TaskMessageRepository, User,
    UserRepository,
};
use std::sync::Arc;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let _network = fdb_layer::foundationdb::boot();
    let db = Database::default().expect("failed to open FDB database");
    let dir = Subspace::all().subspace(&("conformance_v1",));

    let store = Arc::new(RecordStore::new());
    let user_repo = UserRepository::new(store.clone());
    let product_repo = ProductRepository::new(store.clone());
    let post_repo = PostRepository::new(store.clone());
    let task_repo = TaskMessageRepository::new(store.clone());
    let counter_repo = CounterRepository::new(store.clone());

    // 1. Sync metadata and verify all 5 types registered by Go exist
    {
        let tr = db.create_trx().unwrap();
        store
            .sync_metadata(
                &tr,
                &dir,
                &["User", "Product", "Post", "TaskMessage", "Counter"],
            )
            .await
            .expect("sync_metadata failed in Rust");
        tr.commit().await.unwrap();
    }

    let meta = store.metadata();
    assert_eq!(meta.len(), 5, "expected 5 metadata entries, got {meta:?}");

    // 2. Read and verify records written by Go
    {
        let tr = db.create_trx().unwrap();

        let u1 = user_repo
            .get(&tr, &dir, "u1")
            .await
            .expect("failed to get u1 written by Go");
        assert_eq!(u1.name, "Alice");
        assert_eq!(u1.email, "alice@example.com");

        let by_email = user_repo
            .get_user_by_email(&tr, &dir, "alice@example.com")
            .await
            .expect("get_user_by_email failed");
        assert_eq!(by_email.len(), 1);
        assert_eq!(by_email[0].id, "u1");

        let batch_users = user_repo
            .batch_get_user(
                &tr,
                &dir,
                &[
                    vec![Element::String("u1".into())],
                    vec![Element::String("u2".into())],
                ],
            )
            .await
            .expect("batch_get_user failed");
        assert_eq!(batch_users.len(), 2);

        let hw_products = product_repo
            .get_product_by_category(&tr, &dir, "hardware")
            .await
            .expect("get_product_by_category failed");
        assert_eq!(hw_products.len(), 1);
        assert_eq!(hw_products[0].id, "prod1");
        assert_eq!(hw_products[0].price, 150);

        let go_posts = post_repo
            .get_post_by_tags(&tr, &dir, "golang")
            .await
            .expect("get_post_by_tags(golang) failed");
        assert_eq!(go_posts.len(), 1);
        assert_eq!(go_posts[0].id, "post1");

        let cnt1 = counter_repo
            .get(&tr, &dir, "cnt1")
            .await
            .expect("failed to get cnt1 written by Go");
        assert_eq!(cnt1.value, 125);
        assert_eq!(cnt1.max_value, 750);
        assert_eq!(cnt1.min_value, 10);
    }

    // 3. Dequeue the first task enqueued by Go
    {
        let tr = db.create_trx().unwrap();
        let dequeued = task_repo
            .dequeue(&tr, &dir, "shared_queue")
            .await
            .expect("dequeue failed")
            .expect("expected task in shared_queue");
        tr.commit().await.unwrap();

        assert_eq!(dequeued.payload, b"from-go-1");
        assert_eq!(dequeued.shard_id, 7);
        assert_eq!(dequeued.versionstamp.len(), 12);
    }

    // 4. Mutate records, indexes, counters, and queue from Rust
    {
        let tr = db.create_trx().unwrap();

        // Update u1 (changes secondary index on email)
        user_repo
            .set(
                &tr,
                &dir,
                &User {
                    id: "u1".into(),
                    name: "Alice (updated by Rust)".into(),
                    email: "alice.rust@example.com".into(),
                },
            )
            .await
            .unwrap();

        // Delete u2 (clears secondary index on email)
        user_repo.delete(&tr, &dir, "u2").await.unwrap();

        // Update post1 tags (delta fan-out index update: remove "golang", add "conformance")
        post_repo
            .set(
                &tr,
                &dir,
                &Post {
                    id: "post1".into(),
                    tags: vec!["fdb".into(), "rust".into(), "conformance".into()],
                },
            )
            .await
            .unwrap();

        // Create a new Product from Rust
        product_repo
            .create(
                &tr,
                &dir,
                &Product {
                    id: "prod2".into(),
                    name: "Rust Book".into(),
                    category: "books".into(),
                    price: 45,
                },
            )
            .await
            .unwrap();

        // Apply atomic mutations on cnt1 from Rust
        counter_repo
            .add_counter_value(&tr, &dir, "cnt1", 75)
            .await
            .unwrap();
        counter_repo
            .max_counter_max_value(&tr, &dir, "cnt1", 1000)
            .await
            .unwrap();
        counter_repo
            .min_counter_min_value(&tr, &dir, "cnt1", 5)
            .await
            .unwrap();

        // Enqueue a new task from Rust
        task_repo
            .enqueue(
                &tr,
                &dir,
                &TaskMessage {
                    queue_name: "shared_queue".into(),
                    shard_id: 9,
                    versionstamp: vec![],
                    payload: b"from-rust-3".to_vec(),
                },
            )
            .await
            .unwrap();

        tr.commit().await.unwrap();
    }
}
