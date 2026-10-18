use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
};
use falkordb::{FalkorAsyncClient, FalkorClientBuilder, FalkorConnectionInfo};
use futures::TryStreamExt;
use serde_json::{Value, json};
use std::{
    env,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tower_http::cors::CorsLayer;

struct AppState {
    falkor_client: FalkorAsyncClient,
    groq_api_key: String,
    helius_api_key: String,
    http_client: reqwest::Client,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenvy::dotenv().ok();

    let connection_string = env::var("FALKORDB_URL")?;

    let conn_info: FalkorConnectionInfo = connection_string
        .as_str()
        .try_into()
        .map_err(|e| format!("Invalid FalkorDB connection URL: {e}"))?;

    println!("Connecting to FalkorDB Cloud...");

    let client = FalkorClientBuilder::new_async()
        .with_connection_info(conn_info)
        .with_connect_timeout(Some(std::time::Duration::from_secs(30)))
        .build()
        .await?;

    let graphs = client.list_graphs().await?;
    println!("Connected to FalkorDB Cloud.");
    println!("Existing graphs: {:?}", graphs);

    let groq_api_key = env::var("GROQ_API_KEY").map_err(|_| "GROQ_API_KEY must be set in .env")?;
    let helius_api_key =
        env::var("HELIUS_API_KEY").map_err(|_| "HELIUS_API_KEY must be set in .env")?;
    let state = Arc::new(AppState {
        falkor_client: client,
        groq_api_key,
        helius_api_key,
        http_client: reqwest::Client::new(),
    });

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/api/detect-wash-trading", post(detect_wash_trading))
        .route("/api/seed-demo", post(seed_demo))
        .route("/api/generate-risk-report", post(generate_risk_report))
        .route(
            "/api/solana/wallet/{address}/transactions",
            get(get_solana_transactions),
        )
        .route(
            "/api/solana/wallet/{address}/ingest",
            post(ingest_solana_transactions),
        )
        .route("/api/investigations/{id}", get(get_investigation))
        .layer(CorsLayer::permissive())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;

    println!("Axum backend listening on http://localhost:8080");

    axum::serve(listener, app).await?;

    Ok(())
}

async fn health_check() -> &'static str {
    "OK"
}

// Escape a value as a Cypher string literal.
// Dynamic values must never be inserted into queries unescaped.
fn cypher_quote(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());

    for ch in value.chars() {
        match ch {
            '\\' => escaped.push_str("\\\\"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c.is_control() => escaped.push(' '),
            c => escaped.push(c),
        }
    }

    format!("\"{}\"", escaped)
}

// Create synthetic demo wallets and transfer relationships.
async fn seed_demo(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut graph = state.falkor_client.select_graph("defi_risk");

    let query = r#"
        CREATE
          (w1:Wallet {
              address: 'demo_wallet_1',
              risk_score: 0.15
          }),
          (w2:Wallet {
              address: 'demo_wallet_2',
              risk_score: 0.20
          }),
          (w3:Wallet {
              address: 'demo_wallet_3',
              risk_score: 0.10
          }),
          (p:LiquidityPool {
              pair: 'SOL/USDC',
              address: 'DemoPool'
          }),
          (w1)-[:TRANSFERRED_TO {amount: 250.0}]->(w2),
          (w2)-[:TRANSFERRED_TO {amount: 248.5}]->(w3),
          (w3)-[:TRANSFERRED_TO {amount: 247.0}]->(w1),
          (w1)-[:SWAPPED {amount: 1000.0}]->(p)
    "#;

    graph.query(query).execute().await.map_err(api_error)?;

    Ok(Json(json!({
        "status": "success",
        "message": "Demo wallet cycle created",
        "graph": "defi_risk"
    })))
}

// Detect circular transfer paths.
async fn detect_wash_trading(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut graph = state.falkor_client.select_graph("defi_risk");

    let query = r#"
    MATCH p = (a:Wallet)-[:TRANSFERRED_TO*3..6]->(a)
    WHERE ALL(
        r IN relationships(p)
        WHERE r.source = "helius_mainnet"
    )
    RETURN DISTINCT
        a.address AS wallet,
        length(p) AS hop_count
    LIMIT 10
"#;

    let mut result = graph.ro_query(query).execute().await.map_err(api_error)?;

    let mut cycles = Vec::new();

    while let Some(row) = result.data.try_next().await.map_err(api_error)? {
        let wallet: String = row.try_get("wallet").map_err(api_error)?;

        let hop_count: i64 = row.try_get("hop_count").map_err(api_error)?;

        cycles.push(json!({
            "wallet": wallet,
            "hop_count": hop_count
        }));
    }

    Ok(Json(json!({
        "status": "success",
        "detected_cycles": cycles.len(),
        "cycles": cycles,
        "message":
            "Circular transfer patterns are risk signals, not proof of wash trading."
    })))
}

// Generate a risk report and save its investigation memory.
async fn generate_risk_report(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut graph = state.falkor_client.select_graph("defi_risk");

    let query = r#"
        MATCH path =
            (start:Wallet)-[transfers:TRANSFERRED_TO*3..6]->(start)
        WHERE ALL(t IN transfers
            WHERE t.source = "helius_mainnet")
        RETURN DISTINCT
            start.address AS wallet,
            length(path) AS hop_count
        LIMIT 10
    "#;

    let mut result = graph.ro_query(query).execute().await.map_err(api_error)?;

    let mut cycles = Vec::<Value>::new();

    while let Some(row) = result.data.try_next().await.map_err(api_error)? {
        let wallet: String = row.try_get("wallet").map_err(api_error)?;

        let hop_count: i64 = row.try_get("hop_count").map_err(api_error)?;

        cycles.push(json!({
            "wallet": wallet,
            "hop_count": hop_count
        }));
    }

    // The LLM explains the graph evidence; it does not create it.
    let request_body = json!({
        "model": "qwen/qwen3.8-27b",
        "messages": [
            {
                "role": "system",
                "content": "You are a DeFi security analyst. \
                Analyze only the supplied graph evidence. \
                Distinguish observations from hypotheses. \
                Circular transfers alone do not prove wash trading. \
                Do not invent transaction amounts, timestamps, \
                wallet ownership, or other missing evidence."
            },
            {
                "role": "user",
                "content": format!(
                    "Analyze these circular transfer patterns: {}",
                    serde_json::to_string(&cycles).map_err(api_error)?
                )
            }
        ],
        "temperature": 0.2
    });

    let response = state
        .http_client
        .post("https://api.groq.com/openai/v1/chat/completions")
        .bearer_auth(&state.groq_api_key)
        .json(&request_body)
        .send()
        .await
        .map_err(api_error)?;

    let status = response.status();
    let body: Value = response.json().await.map_err(api_error)?;

    if !status.is_success() {
        eprintln!("Groq API returned HTTP {status}: {body}");

        return Err((
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "status": "error",
                "error": "Groq could not generate the risk report."
            })),
        ));
    }

    let report = body
        .pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .ok_or_else(|| api_error("Groq response did not contain message content"))?
        .to_owned();

    // Create a unique ID and timestamp for this investigation.
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(api_error)?;

    let investigation_id = now.as_nanos().to_string();
    let created_at = now.as_millis();

    let evidence_json = serde_json::to_string(&cycles).map_err(api_error)?;

    // 1. Save the investigation itself.
    let create_investigation = format!(
        r#"
        CREATE (i:Investigation {{
            id: {},
            status: "completed",
            created_at: {},
            evidence_json: {},
            report: {}
        }})
        "#,
        cypher_quote(&investigation_id),
        created_at,
        cypher_quote(&evidence_json),
        cypher_quote(&report),
    );

    graph
        .query(&create_investigation)
        .execute()
        .await
        .map_err(api_error)?;

    // 2. Create a Finding and connect it to the investigation.
    let create_finding = format!(
        r#"
        MATCH (i:Investigation {{id: {}}})
        CREATE (f:Finding {{
            investigation_id: {},
            type: "circular_transfer_pattern",
            explanation: {}
        }})
        CREATE (i)-[:PRODUCED]->(f)
        "#,
        cypher_quote(&investigation_id),
        cypher_quote(&investigation_id),
        cypher_quote(&report),
    );

    graph
        .query(&create_finding)
        .execute()
        .await
        .map_err(api_error)?;

    // 3. Connect each observed wallet and save its evidence.
    for item in &cycles {
        let Some(wallet) = item["wallet"].as_str() else {
            continue;
        };

        let Some(hop_count) = item["hop_count"].as_i64() else {
            continue;
        };

        let save_evidence = format!(
            r#"
            MATCH (i:Investigation {{id: {}}})
            MATCH (f:Finding {{investigation_id: {}}})
            MERGE (w:Wallet {{address: {}}})
            MERGE (i)-[:INVOLVES]->(w)
            CREATE (e:Evidence {{
                investigation_id: {},
                evidence_type: "circular_transfer_cycle",
                wallet: {},
                hop_count: {},
                source: "FalkorDB graph traversal",
                observed_at: {}
            }})
            CREATE (f)-[:SUPPORTED_BY]->(e)
            MERGE (e)-[:ABOUT_WALLET]->(w)
            "#,
            cypher_quote(&investigation_id),
            cypher_quote(&investigation_id),
            cypher_quote(wallet),
            cypher_quote(&investigation_id),
            cypher_quote(wallet),
            hop_count,
            created_at,
        );

        graph
            .query(&save_evidence)
            .execute()
            .await
            .map_err(api_error)?;
    }

    // 4. Link this case to older investigations that share a wallet.
    // This is a graph connection, not proof that the cases are malicious.
    let related_query = format!(
        r#"
        MATCH (current:Investigation {{id: {}}})-[:INVOLVES]->(w:Wallet)
              <-[:INVOLVES]-(previous:Investigation)
        WHERE previous.id <> {}
        MERGE (current)-[:RELATED_TO]->(previous)
        "#,
        cypher_quote(&investigation_id),
        cypher_quote(&investigation_id),
    );

    graph
        .query(&related_query)
        .execute()
        .await
        .map_err(api_error)?;

    Ok(Json(json!({
        "status": "success",
        "investigation_id": investigation_id,
        "wallets_flagged": cycles.len(),
        "graph_evidence": cycles,
        "ai_risk_report": report
    })))
}

// Retrieve a saved investigation and its related previous cases.
async fn get_investigation(
    State(state): State<Arc<AppState>>,
    Path(id): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    let mut graph = state.falkor_client.select_graph("defi_risk");

    let query = format!(
        r#"
        MATCH (i:Investigation {{id: {}}})
        RETURN i.id AS id,
               i.status AS status,
               i.created_at AS created_at,
               i.report AS report,
               i.evidence_json AS evidence_json
        "#,
        cypher_quote(&id),
    );

    let mut result = graph.ro_query(&query).execute().await.map_err(api_error)?;

    let Some(row) = result.data.try_next().await.map_err(api_error)? else {
        return Err((
            StatusCode::NOT_FOUND,
            Json(json!({
                "status": "error",
                "error": "Investigation not found"
            })),
        ));
    };

    let saved_id: String = row.try_get("id").map_err(api_error)?;

    let saved_status: String = row.try_get("status").map_err(api_error)?;

    let created_at: i64 = row.try_get("created_at").map_err(api_error)?;

    let report: String = row.try_get("report").map_err(api_error)?;

    let evidence_json: String = row.try_get("evidence_json").map_err(api_error)?;

    let evidence: Value = serde_json::from_str(&evidence_json).map_err(api_error)?;

    // Read previous investigations connected through shared wallets.
    let related_query = format!(
        r#"
        MATCH (i:Investigation {{id: {}}})-[:RELATED_TO]->
              (previous:Investigation)
        RETURN DISTINCT previous.id AS id,
                        previous.created_at AS created_at
        LIMIT 20
        "#,
        cypher_quote(&id),
    );

    let mut related_result = graph
        .ro_query(&related_query)
        .execute()
        .await
        .map_err(api_error)?;

    let mut related = Vec::new();

    while let Some(row) = related_result.data.try_next().await.map_err(api_error)? {
        let previous_id: String = row.try_get("id").map_err(api_error)?;

        let previous_created_at: i64 = row.try_get("created_at").map_err(api_error)?;

        related.push(json!({
            "investigation_id": previous_id,
            "created_at": previous_created_at
        }));
    }

    Ok(Json(json!({
        "status": "success",
        "investigation": {
            "id": saved_id,
            "status": saved_status,
            "created_at": created_at,
            "ai_risk_report": report,
            "graph_evidence": evidence
        },
        "related_previous_investigations": related
    })))
}

fn api_error<E: std::fmt::Display>(error: E) -> (StatusCode, Json<Value>) {
    eprintln!("Backend Error: {error}");

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "status": "error",
            "error": error.to_string()
        })),
    )
}
// Fetch recent parsed Solana transactions for a wallet using Helius.
async fn get_solana_transactions(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if address.is_empty() || address.len() > 64 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid wallet address"})),
        ));
    }

    let url = format!(
        "https://api-mainnet.helius-rpc.com/v0/addresses/{}/transactions?api-key={}",
        address, state.helius_api_key
    );

    let response = state
        .http_client
        .get(&url)
        .query(&[("limit", "10")])
        .send()
        .await
        .map_err(|_| {
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "Could not connect to Helius"})),
            )
        })?;

    let status = response.status();

    if !status.is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": "Helius returned an unsuccessful response",
                "helius_status": status.as_u16()
            })),
        ));
    }

    let transactions: Value = response.json().await.map_err(|_| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "Could not parse Helius response"})),
        )
    })?;

    Ok(Json(json!({
        "network": "solana-mainnet",
        "wallet": address,
        "count": transactions.as_array().map_or(0, |items| items.len()),
        "transactions": transactions
    })))
}

// Fetch recent Helius transactions and idempotently store transfer edges.
async fn ingest_solana_transactions(
    State(state): State<Arc<AppState>>,
    Path(address): Path<String>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)> {
    if address.is_empty() || address.len() > 64 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Invalid wallet address"})),
        ));
    }

    let url = format!(
        "https://api-mainnet.helius-rpc.com/v0/addresses/{}/transactions",
        address
    );

    let response = state
        .http_client
        .get(&url)
        .query(&[("api-key", state.helius_api_key.as_str()), ("limit", "10")])
        .send()
        .await
        .map_err(|_| {
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "Could not connect to Helius"})),
            )
        })?;

    let status = response.status();

    if !status.is_success() {
        return Err((
            StatusCode::BAD_GATEWAY,
            Json(json!({
                "error": "Helius returned an unsuccessful response",
                "helius_status": status.as_u16()
            })),
        ));
    }

    let transactions: Value = response.json().await.map_err(|_| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "Could not parse Helius response"})),
        )
    })?;

    let tx_list = transactions.as_array().ok_or_else(|| {
        (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error": "Unexpected Helius response format"})),
        )
    })?;

    let mut graph = state.falkor_client.select_graph("defi_risk");
    let mut transfers_imported = 0usize;
    let mut transactions_processed = 0usize;

    for tx in tx_list {
        let signature = match tx.get("signature").and_then(Value::as_str) {
            Some(value) if !value.is_empty() => value,
            _ => continue,
        };

        transactions_processed += 1;

        let timestamp = tx.get("timestamp").and_then(Value::as_i64).unwrap_or(0);

        // Native transfers report amounts in lamports; 1 SOL = 1e9 lamports.
        if let Some(transfers) = tx.get("nativeTransfers").and_then(Value::as_array) {
            for (index, transfer) in transfers.iter().enumerate() {
                let from = transfer
                    .get("fromUserAccount")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let to = transfer
                    .get("toUserAccount")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let lamports = match transfer.get("amount").and_then(Value::as_f64) {
                    Some(value) if value >= 0.0 => value,
                    _ => continue,
                };

                if from.is_empty() || to.is_empty() {
                    continue;
                }

                let sol_amount = lamports / 1_000_000_000.0;

                let query = format!(
                    r#"
                    MERGE (sender:Wallet {{address: {}}})
                    MERGE (receiver:Wallet {{address: {}}})
                    MERGE (sender)-[t:TRANSFERRED_TO {{
                        signature: {},
                        transfer_kind: "native",
                        transfer_index: {}
                    }}]->(receiver)
                    SET t.amount = {},
                        t.asset = "SOL",
                        t.raw_amount_lamports = {},
                        t.block_time = {},
                        t.source = "helius_mainnet"
                    "#,
                    cypher_quote(from),
                    cypher_quote(to),
                    cypher_quote(signature),
                    index,
                    sol_amount,
                    lamports,
                    timestamp
                );

                graph.query(query).execute().await.map_err(api_error)?;
                transfers_imported += 1;
            }
        }

        // Token transfers retain their mint so different assets stay distinguishable.
        if let Some(transfers) = tx.get("tokenTransfers").and_then(Value::as_array) {
            for (index, transfer) in transfers.iter().enumerate() {
                let from = transfer
                    .get("fromUserAccount")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let to = transfer
                    .get("toUserAccount")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let mint = transfer.get("mint").and_then(Value::as_str).unwrap_or("");
                let amount = match transfer.get("tokenAmount").and_then(Value::as_f64) {
                    Some(value) if value >= 0.0 => value,
                    _ => continue,
                };

                if from.is_empty() || to.is_empty() || mint.is_empty() {
                    continue;
                }

                let query = format!(
                    r#"
                    MERGE (sender:Wallet {{address: {}}})
                    MERGE (receiver:Wallet {{address: {}}})
                    MERGE (sender)-[t:TRANSFERRED_TO {{
                        signature: {},
                        transfer_kind: "token",
                        transfer_index: {}
                    }}]->(receiver)
                    SET t.amount = {},
                        t.asset = "TOKEN",
                        t.mint = {},
                        t.block_time = {},
                        t.source = "helius_mainnet"
                    "#,
                    cypher_quote(from),
                    cypher_quote(to),
                    cypher_quote(signature),
                    index,
                    amount,
                    cypher_quote(mint),
                    timestamp
                );

                graph.query(query).execute().await.map_err(api_error)?;
                transfers_imported += 1;
            }
        }
    }

    Ok(Json(json!({
        "status": "success",
        "network": "solana-mainnet",
        "wallet": address,
        "transactions_processed": transactions_processed,
        "transfers_imported": transfers_imported,
        "graph": "defi_risk",
        "message": "Transfers imported. Repeated imports reuse matching wallet and transfer patterns."
    })))
}
