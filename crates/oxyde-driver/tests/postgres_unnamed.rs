//! PostgreSQL regression tests, explicitly enabled against a disposable database:
//! `OXYDE_TEST_POSTGRES_URL=postgresql://... cargo test -p oxyde-driver
//! --test postgres_unnamed -- --ignored`

use serde_json::json;
use sqlx::postgres::{PgConnectOptions, PgSslMode, PgTypeInfo};
use sqlx::types::Json;
use sqlx::{Connection, Executor, PgConnection, Row, Statement};

fn connect_options(cache_capacity: usize) -> PgConnectOptions {
    std::env::var("OXYDE_TEST_POSTGRES_URL")
        .expect("set OXYDE_TEST_POSTGRES_URL to a disposable PostgreSQL database")
        .parse::<PgConnectOptions>()
        .unwrap()
        .statement_cache_capacity(cache_capacity)
}

async fn connect(cache_capacity: usize) -> PgConnection {
    PgConnection::connect_with(&connect_options(cache_capacity))
        .await
        .unwrap()
}

#[tokio::test]
#[ignore = "requires OXYDE_TEST_POSTGRES_URL"]
async fn cache_disabled_queries_follow_ddl_without_named_statements() {
    let mut conn = connect(0).await;
    conn.execute("CREATE TEMP TABLE unnamed_probe (value VARCHAR(32))")
        .await
        .unwrap();
    for _ in 0..3 {
        let row = sqlx::query("INSERT INTO unnamed_probe (value) VALUES ($1) RETURNING *")
            .bind("before")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("value"), "before");
        sqlx::query("SELECT * FROM unnamed_probe")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    }
    conn.execute("ALTER TABLE unnamed_probe ALTER COLUMN value TYPE TEXT")
        .await
        .unwrap();
    conn.execute("ALTER TABLE unnamed_probe ADD COLUMN extra INT DEFAULT 42")
        .await
        .unwrap();
    for _ in 0..3 {
        let row = sqlx::query("INSERT INTO unnamed_probe (value) VALUES ($1) RETURNING *")
            .bind("after")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("value"), "after");
        assert_eq!(row.get::<i32, _>("extra"), 42);
        let rows = sqlx::query("SELECT * FROM unnamed_probe")
            .fetch_all(&mut conn)
            .await
            .unwrap();
        assert!(rows.iter().all(|row| row.get::<i32, _>("extra") == 42));
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_prepared_statements")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[derive(sqlx::Type, Debug, PartialEq)]
#[sqlx(type_name = "pg_temp.probe_mood", rename_all = "lowercase")]
enum Mood {
    Happy,
}

#[tokio::test]
#[ignore = "requires OXYDE_TEST_POSTGRES_URL"]
async fn unnamed_binary_arguments_and_error_recovery() {
    let mut conn = connect(0).await;
    conn.execute("CREATE TEMP TABLE types_probe (id INT)")
        .await
        .unwrap();
    conn.execute("CREATE TYPE pg_temp.probe_mood AS ENUM ('happy')")
        .await
        .unwrap();

    let row = sqlx::query("SELECT $1::json AS json, $2::jsonb AS jsonb, $3::int4[] AS ints, $4::text AS absent, $5::text AS mood, $6::text[] AS moods")
        .bind(Json(json!({"nested": [true, 42]})))
        .bind(Json(json!({"nested": [true, 42]})))
        .bind(vec![1_i32, 2, 3])
        .bind(None::<String>)
        .bind(Mood::Happy)
        .bind(vec![Mood::Happy])
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        row.get::<Json<serde_json::Value>, _>("json").0,
        json!({"nested": [true, 42]})
    );
    assert_eq!(
        row.get::<Json<serde_json::Value>, _>("jsonb").0,
        json!({"nested": [true, 42]})
    );
    assert_eq!(row.get::<Vec<i32>, _>("ints"), vec![1, 2, 3]);
    assert_eq!(row.get::<Option<String>, _>("absent"), None);
    assert_eq!(row.get::<String, _>("mood"), "happy");
    assert_eq!(row.get::<Vec<String>, _>("moods"), vec!["happy"]);

    assert!(sqlx::query("SELECT 1 / $1")
        .bind(0_i32)
        .fetch_one(&mut conn)
        .await
        .is_err());
    assert!(sqlx::query("SELECT FROM WHERE")
        .fetch_one(&mut conn)
        .await
        .is_err());
    assert_eq!(
        sqlx::query_scalar::<_, i32>("SELECT $1")
            .bind(42_i32)
            .persistent(false)
            .fetch_one(&mut conn)
            .await
            .unwrap(),
        42
    );

    // `&str` execution keeps the simple-query path working after an error.
    assert_eq!(
        conn.fetch_one("SELECT 42 AS value")
            .await
            .unwrap()
            .get::<i32, _>("value"),
        42
    );
}

#[tokio::test]
#[ignore = "requires OXYDE_TEST_POSTGRES_URL"]
async fn cached_and_explicitly_prepared_statements_keep_existing_behavior() {
    let mut cached = connect(8).await;
    for value in 1..4 {
        assert_eq!(
            sqlx::query_scalar::<_, i32>("SELECT $1")
                .bind(value)
                .fetch_one(&mut cached)
                .await
                .unwrap(),
            value
        );
    }
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_prepared_statements")
        .fetch_one(&mut cached)
        .await
        .unwrap();
    assert!(count > 0);

    let mut uncached = connect(0).await;
    let statement = uncached
        .prepare_with("SELECT $1 AS value", &[PgTypeInfo::with_name("int4")])
        .await
        .unwrap();
    let row = statement
        .query()
        .bind(42_i32)
        .fetch_one(&mut uncached)
        .await
        .unwrap();
    assert_eq!(row.get::<i32, _>("value"), 42);
}

#[tokio::test]
#[ignore = "requires OXYDE_TEST_POSTGRES_URL"]
async fn unnamed_query_sends_parse_through_execute_before_one_sync() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;

    let options = connect_options(0).ssl_mode(PgSslMode::Disable);
    let remote = TcpStream::connect((options.get_host(), options.get_port()))
        .await
        .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (sender, receiver) = oneshot::channel();
    let proxy = tokio::spawn(async move {
        let (mut client, _) = listener.accept().await.unwrap();
        let mut remote = remote;
        // Startup has no message tag. All later frontend messages do.
        let length = client.read_u32().await.unwrap();
        let mut startup = vec![0; length as usize - 4];
        client.read_exact(&mut startup).await.unwrap();
        remote.write_u32(length).await.unwrap();
        remote.write_all(&startup).await.unwrap();
        let (mut client_read, mut client_write) = client.into_split();
        let (mut remote_read, mut remote_write) = remote.into_split();
        let responses =
            tokio::spawn(async move { tokio::io::copy(&mut remote_read, &mut client_write).await });
        let mut sender = Some(sender);
        let mut batch = Vec::new();
        let mut probing = false;
        while let Ok(tag) = client_read.read_u8().await {
            let length = client_read.read_u32().await.unwrap();
            let mut body = vec![0; length as usize - 4];
            client_read.read_exact(&mut body).await.unwrap();
            if tag == b'P' && body.windows(14).any(|part| part == b"one_trip_probe") {
                probing = true;
            }
            if probing {
                batch.push((tag, body.clone()));
            }
            if tag == b'S' && probing {
                sender
                    .take()
                    .unwrap()
                    .send(std::mem::take(&mut batch))
                    .unwrap();
                probing = false;
            }
            remote_write.write_u8(tag).await.unwrap();
            remote_write.write_u32(length).await.unwrap();
            remote_write.write_all(&body).await.unwrap();
            if tag == b'X' {
                break;
            }
        }
        responses.abort();
    });

    let mut conn = PgConnection::connect_with(&options.host("127.0.0.1").port(port))
        .await
        .unwrap();
    let value: i32 = sqlx::query_scalar("SELECT $1 AS one_trip_probe")
        .bind(42_i32)
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(value, 42);
    let batch = tokio::time::timeout(std::time::Duration::from_secs(5), receiver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        batch.iter().map(|(tag, _)| *tag).collect::<Vec<_>>(),
        b"PBDECS"
    );
    assert_eq!(batch[0].1[0], 0, "Parse must use an unnamed statement");
    assert_eq!(
        &batch[1].1[..2],
        &[0, 0],
        "Bind portal and statement must be unnamed"
    );
    assert_eq!(
        batch[2].1, b"P\0",
        "Describe must target the unnamed portal"
    );
    conn.close().await.unwrap();
    proxy.await.unwrap();
}
