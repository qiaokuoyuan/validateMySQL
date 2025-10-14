use bincode::{deserialize, serialize};
use clap::Parser;
use mysql_async::prelude::*;
use mysql_async::*;
use regex::Regex;
use rust_xlsxwriter::Workbook;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fs;
use url::form_urlencoded;

#[derive(Parser, Debug)]
#[command(version, about = "MYSQL表结构校验工具")]
struct Args {
    #[arg(short, long, default_value = "", help = "输入文件")]
    input_file: String,

    #[arg(short, long, default_value = "", help = "输出文件")]
    output_file: String,

    #[arg(short, long, default_value_t = false, help = "创建数据缓存模式")]
    create: bool,

    #[arg(short, long, default_value_t = false, help = "基于缓存验证标结构模式")]
    validate: bool,

    #[arg(short, long, default_value_t = false, help = "验证sql模式")]
    execute_sql: bool,

    #[arg(short = 'H', long, default_value = "", help = "MySQL 主机地址")]
    host: String,

    #[arg(short = 'u', long, default_value = "", help = "MySQL 主机账号")]
    user: String,

    #[arg(short = 'P', long, default_value_t = 0, help = "MySQL 主机端口")]
    port: u16,

    #[arg(short = 'p', long, default_value = "", help = "MySQL 主机密码")]
    password: String,

    #[arg(short = 'd', long, default_value = "", help = "MySQL 主机库名")]
    database: String,

    #[arg(
        short = 'x',
        long,
        default_value_t = false,
        help = "输出修补sql文件位置，注意：只会生成修补列的sql"
    )]
    fix_lost_cols: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct ColInfo {
    col_name: String,
    col_type: String,
    is_nullable: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct TableInfo {
    table_name: String,
    col_infos: Vec<ColInfo>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
struct DBInfo {
    db_name: String,
    table_infos: Vec<TableInfo>,
}

fn encode_str(s: &str) -> String {
    form_urlencoded::byte_serialize(s.as_bytes()).collect()
}

/* ---------- 数据库访问层（异步） ---------- */
async fn get_db_table_names(conn: &mut Conn, db_name: &str) -> Result<Vec<String>> {
    let sql = format!(
        "SELECT DISTINCT TABLE_NAME \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = '{}'",
        db_name
    );
    conn.query(sql).await
}

async fn get_table_info(conn: &mut Conn, db_name: &str, table_name: &str) -> Result<TableInfo> {
    let sql = format!(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE \
         FROM information_schema.COLUMNS \
         WHERE TABLE_SCHEMA = '{}' AND TABLE_NAME = '{}'",
        db_name, table_name
    );
    let col_infos: Vec<ColInfo> = conn
        .query(sql)
        .await?
        .into_iter()
        .map(|(col_name, col_type, is_nullable)| ColInfo {
            col_name,
            col_type,
            is_nullable,
        })
        .collect();
    Ok(TableInfo {
        table_name: table_name.to_string(),
        col_infos,
    })
}

async fn get_db_info(conn: &mut Conn, db_name: &str) -> Result<DBInfo> {
    let table_names = get_db_table_names(conn, db_name).await?;
    let mut table_infos = Vec::new();
    for table_name in table_names {
        table_infos.push(get_table_info(conn, db_name, &table_name).await?);
    }
    Ok(DBInfo {
        db_name: db_name.to_string(),
        table_infos,
    })
}

/* ---------- 业务逻辑（异步） ---------- */
async fn create_db_info(pool: &Pool, db_name: String, output_path: String) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let db_info = get_db_info(&mut conn, &db_name).await?;
    let bytes = serialize(&db_info).unwrap();
    fs::write(output_path, bytes)?;
    Ok(())
}

async fn validate_db_info(
    pool: &Pool,
    db_name: String,
    cache_file: String,
    output_xlsx: String,
    fix_lost_cols: bool,
) -> Result<()> {
    let mut conn = pool.get_conn().await?;
    let current = get_db_info(&mut conn, &db_name).await?;
    let cached: DBInfo = deserialize(&fs::read(cache_file)?).unwrap();

    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    let mut row = 0;

    let mut write_row = |c1: &str, c2: &str, c3: &str, c4: &str, c5: &str| {
        ws.write_string(row, 0, c1).unwrap();
        ws.write_string(row, 1, c2).unwrap();
        ws.write_string(row, 2, c3).unwrap();
        ws.write_string(row, 3, c4).unwrap();
        ws.write_string(row, 4, c5).unwrap();
        row += 1;
    };

    write_row("数据库", "表名", "列名", "比较结果", "比较信息");

    let all_tables: HashSet<_> = cached
        .table_infos
        .iter()
        .chain(current.table_infos.iter())
        .map(|t| &t.table_name)
        .collect();

    // 修补列的sql计集合
    let mut fix_cols_sqls = vec![];

    // 比较每一张表
    for tbl in all_tables {
        // 缓存表
        let cached_tbl = cached.table_infos.iter().find(|t| {
            let table_name = t.table_name.to_lowercase();
            tbl.to_lowercase() == table_name
        });

        // 当前表
        let curr_tbl = current.table_infos.iter().find(|t| {
            let table_name = t.table_name.to_lowercase();
            tbl.to_lowercase() == table_name
        });

        match (cached_tbl, curr_tbl) {
            // 两张表都存在
            (Some(c), Some(n)) => {
                // 只要一个列在2侧列中任意一个存在，就参与比较
                let all_cols: HashSet<_> = c
                    .col_infos
                    .iter()
                    .chain(n.col_infos.iter())
                    .map(|c| &c.col_name)
                    .collect();

                // 循环每个要比较的列
                for col in all_cols {
                    let cached_col = c.col_infos.iter().find(|x| {
                        let col_name = x.col_name.to_lowercase();
                        col_name == col.to_lowercase()
                    });
                    let curr_col = n.col_infos.iter().find(|x| {
                        let col_name = x.col_name.to_lowercase();
                        col_name == col.to_lowercase()
                    });
                    match (cached_col, curr_col) {
                        (Some(old), Some(new)) if old.col_type == new.col_type => {
                            write_row(&db_name, tbl, col, "成功", "");
                        }
                        (Some(old), Some(new)) => {
                            write_row(
                                &db_name,
                                tbl,
                                col,
                                "失败",
                                &format!("列定义不一致{} --> {}", old.col_type, new.col_type),
                            );
                        }

                        (Some(_), None) => {
                            write_row(&db_name, tbl, col, "失败", "列缺失");

                            // 如果需要添加修复列sql
                            if fix_lost_cols {
                                let table_name = curr_tbl.unwrap().table_name.clone();
                                let col_name = cached_col.unwrap().col_name.clone();
                                let col_type = cached_col.unwrap().col_type.clone();

                                let sql = format!(
                                    "alter table {table_name} add column {col_name} {col_type};"
                                );

                                fix_cols_sqls.push(sql);
                            }
                        }

                        (None, Some(_)) => {
                            write_row(&db_name, tbl, col, "失败", "列新增");
                        }

                        _ => {}
                    }
                }
            }

            // 缓存表存在，当前表不存在
            (Some(_), None) => write_row(&db_name, tbl, "", "失败", "表缺失"),

            // 缓存表不存在，当前表存在
            (None, Some(_)) => write_row(&db_name, tbl, "", "失败", "表新增"),

            // 其他
            _ => write_row(&db_name, tbl, "", "失败", "双侧缺失"),
        }
    }

    // 保存对比结果
    wb.save(output_xlsx).unwrap();

    // 如果有  fix_cols_sqls
    if fix_lost_cols {
        fs::write("./path-cols.sql", fix_cols_sqls.join("\n")).expect("生成修补列sql失败");
    }
    Ok(())
}

async fn execute_sql_list(pool: &Pool, sql_list: &[&str], output_xlsx: &str) -> Result<()> {
    let mut conn = pool.get_conn().await?;

    // 定义写入的 excel
    let mut wb = Workbook::new();
    let ws = wb.add_worksheet();
    let mut row = 0;

    // 定义写入 excel 方法
    let mut write_row = |c1: &str, c2: &str| {
        ws.write_string(row, 0, c1).unwrap();
        ws.write_string(row, 1, c2).unwrap();
        row += 1;
    };

    // 写入表头
    write_row("SQL", "执行结果");

    // 比较每一张表
    for sql in sql_list {
        // 指定sql
        let rst = conn.query::<Row, &str>(sql).await?;

        // 将返回值保存到excel
        let rst = format!("{:?}", rst);

        // 写入行信息
        write_row(sql, rst.as_str());
    }

    // 保存对比结果
    wb.save(output_xlsx).unwrap();

    Ok(())
}

/* ---------- 主入口 ---------- */
#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    println!("指定配置项: {:#?}", args);

    // 生成文件模式
    if args.create {
        let host = if args.host.is_empty() {
            "10.31.79.48".into()
        } else {
            args.host
        };
        let port = if args.port == 0 { 3306 } else { args.port };
        let user = if args.user.is_empty() {
            "chkd".into()
        } else {
            args.user
        };
        let password = if args.password.is_empty() {
            "Chkd@146.48".into()
        } else {
            args.password
        };
        let database = if args.database.is_empty() {
            "yyws_xyzl_view".into()
        } else {
            args.database
        };

        let encoded_pw = encode_str(&password);
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            user, encoded_pw, host, port, database
        );
        println!("使用连接字符串: {}", url);

        let pool = Pool::new(url.as_str());

        let out = if args.output_file.is_empty() {
            "dbInfo.bin"
        } else {
            &args.output_file
        };
        create_db_info(&pool, database, out.into()).await?;
        println!("表结构快照存储到：{}", out);

        // 释放连接池
        pool.disconnect().await?;
    }
    // 验证模式
    else if args.validate {
        let host = if args.host.is_empty() {
            "localhost".into()
        } else {
            args.host
        };
        let port = if args.port == 0 { 3306 } else { args.port };
        let user = if args.user.is_empty() {
            "yywsxyzl".into()
        } else {
            args.user
        };
        let password = if args.password.is_empty() {
            "xyzl2@24".into()
        } else {
            args.password
        };
        let database = if args.database.is_empty() {
            "yyws_xyzl_view".into()
        } else {
            args.database
        };

        let encoded_pw = encode_str(&password);
        let url = format!(
            "mysql://{}:{}@{}:{}/{}",
            user, encoded_pw, host, port, database
        );
        println!("使用连接字符串: {}", url);

        let pool = Pool::new(url.as_str());

        let cache = if args.input_file.is_empty() {
            "dbInfo.bin"
        } else {
            &args.input_file
        };
        let out = if args.output_file.is_empty() {
            "validateResult.xlsx".into()
        } else if Regex::new(r"\.xlsx$").unwrap().is_match(&args.output_file) {
            args.output_file
        } else {
            println!("指定输出文件格式不正确，使用默认 validateResult.xlsx");
            "validateResult.xlsx".into()
        };

        // 是否修复丢失的列
        let fix_lost_cols = args.fix_lost_cols;

        validate_db_info(&pool, database, cache.into(), out.clone(), fix_lost_cols).await?;
        println!("输出文件: {}", out);

        // 释放连接池
        pool.disconnect().await?;
    }
    // 检查sql模式
    else if args.execute_sql {
        // 指定的需要验证sql文件
        let sql_file_dir = args.input_file.as_str();

        // 验证指定了文件名
        if sql_file_dir.is_empty() {
            println!("必须指定包含待验证sql的路径");
            return Ok(());
        }

        // 验证要验证的sql文件是否存在
        if !fs::exists(sql_file_dir)? {
            println!("指定sql文件路径[{}]有误", sql_file_dir);
            return Ok(());
        } else {
            // 验证sql模式下要执行sql的主机配置
            let host = if args.host.is_empty() {
                "localhost".into()
            } else {
                args.host
            };
            let port = if args.port == 0 { 3306 } else { args.port };
            let user = if args.user.is_empty() {
                "yywsxyzl".into()
            } else {
                args.user
            };
            let password = if args.password.is_empty() {
                "xyzl2@24".into()
            } else {
                args.password
            };
            let database = if args.database.is_empty() {
                "yyws_xyzl_view".into()
            } else {
                args.database
            };

            let output_excel = if args.output_file.is_empty() {
                "exeSqlRst.xlsx"
            } else {
                args.output_file.as_str()
            };

            let encoded_pw = encode_str(&password);
            let url = format!(
                "mysql://{}:{}@{}:{}/{}",
                user, encoded_pw, host, port, database
            );
            println!("使用连接字符串: {}", url);

            let pool = Pool::new(url.as_str());

            // 读取所有sql
            let sql_list = fs::read_to_string(sql_file_dir)?;

            // 所有的sql
            let sql_list = sql_list.split(";").collect::<Vec<&str>>();

            // 执行sql并保存结果
            execute_sql_list(&pool, &sql_list, &output_excel).await?;

            // 释放连接池
            pool.disconnect().await?;

            println!("sql集执行结果： {}", output_excel);
        }
    } else {
        println!("请至少选择一个模式： -c 创建表结构快照； -v 基于快照验证  -e 执行sql");
    }
    Ok(())
}
