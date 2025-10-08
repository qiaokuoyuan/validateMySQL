use bincode::{deserialize, serialize};
use clap::Parser;
use itertools::Itertools;
use mysql::prelude::*;
use mysql::*;
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

    #[arg(
        short = 'H',
        long,
        default_value = "localhost",
        help = "MySQL 主机地址"
    )]
    host: String,

    #[arg(short = 'u', long, default_value = "root", help = "MySQL 主机账号")]
    user: String,

    #[arg(short = 'P', long, default_value_t = 3306, help = "MySQL 主机端口")]
    port: i32,

    #[arg(short = 'p', long, default_value = "123456", help = "MySQL 主机密码")]
    password: String,

    #[arg(short = 'd', long, default_value = "p10", help = "MySQL 主机库名")]
    database: String,
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

// 编码字符串
fn encode_str(s: &str) -> String {
    return form_urlencoded::byte_serialize(s.as_bytes()).collect();
}

// 获取一个数据库下所有表名称数组
fn get_db_table_names(con: &mut PooledConn, db_name: String) -> Result<Vec<String>> {
    // 生成查询表sql
    let sql = format!(
        "SELECT DISTINCT TABLE_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db_name}'"
    );

    con.query_map(&sql, |a| a)
}

// 获取一个表的信息
fn get_table_info(con: &mut PooledConn, db_name: String, table_name: String) -> Result<TableInfo> {
    let sql = format!(
        "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE from information_schema.COLUMNS WHERE TABLE_SCHEMA = '{db_name}' AND TABLE_NAME= '{table_name}'"
    );

    // Let's select payments from database. Type inference should do the trick here.
    let col_infos = con.query_map(sql, |(col_name, col_type, is_nullable)| ColInfo {
        col_name,
        col_type,
        is_nullable,
    })?;

    Ok(TableInfo {
        table_name,
        col_infos,
    })
}

// 获取一个数据库的信息
fn get_db_info(con: &mut PooledConn, db_name: String) -> DBInfo {
    // 获取当前数据库下所有表名
    let table_names = get_db_table_names(con, db_name.clone()).unwrap();

    // 存储每个表的信息
    let mut table_infos: Vec<TableInfo> = Vec::new();

    table_names.iter().for_each(|table_name| {
        let table_info = get_table_info(con, db_name.clone(), table_name.clone()).unwrap();
        table_infos.push(table_info);
    });

    DBInfo {
        db_name,
        table_infos,
    }
}

// 将一个数据库的信息写入到本地文件
fn write_db_info_to_files(
    db_info: DBInfo,
    output_dir: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let bytes = serialize(&db_info).unwrap();
    fs::write(output_dir, bytes)?;
    Ok(())
}

fn create_db_info(con: &mut PooledConn, db_name: String, output_dir: String) {
    let db_info = get_db_info(con, db_name);
    write_db_info_to_files(db_info, output_dir).expect("TODO: panic message");
}

// 验证信息是否正确
fn validate_db_info(
    con: &mut PooledConn,
    db_name: String,
    cache_file_dir: String,
    output_dir: String,
) {
    println!("reading cache file: {}", cache_file_dir);

    // 读取缓存的数据库信息
    let read_result = fs::read(cache_file_dir);

    // 检查是否缓存文件能正常读取
    match read_result {
        Ok(bytes) => {
            // 读取当前数据库的信息
            let current_db_info = get_db_info(con, db_name.clone());

            // 读取缓存的数据库信息
            let cached_db_info: DBInfo = deserialize(&bytes).unwrap();

            // 将2个库信息中的表 union 起来
            let mut union_table_names = HashSet::new();
            for d in [&cached_db_info, &current_db_info] {
                for t in &d.table_infos {
                    let table_name = &t.table_name;
                    if !union_table_names.contains(table_name) {
                        union_table_names.insert(table_name.clone());
                    }
                }
            }

            // 新建excel写入文件
            let mut wb = Workbook::new();
            // 2. 添加工作表
            let worksheet = wb.add_worksheet();

            // 当前 excel 写入文件行号
            let mut excel_row_index = 0;

            // 定义excel写入单行函数
            let mut add_excel_row =
                |col1: String, col2: String, col3: String, col4: String, col5: String| {
                    worksheet.write_string(excel_row_index, 0, col1).unwrap();
                    worksheet.write_string(excel_row_index, 1, col2).unwrap();
                    worksheet.write_string(excel_row_index, 2, col3).unwrap();
                    worksheet.write_string(excel_row_index, 3, col4).unwrap();
                    worksheet.write_string(excel_row_index, 4, col5).unwrap();

                    // 行号+1
                    excel_row_index += 1;
                };

            // 写入表头
            add_excel_row(
                "数据库".into(),
                "表名".into(),
                "列名".into(),
                "比较结果".into(),
                "比较信息".into(),
            );

            // 比较每个表
            for table_name in union_table_names {
                // 从数据文件中读取的表信息
                let cached_table_info = cached_db_info
                    .table_infos
                    .iter()
                    .find_or_first(|a| a.table_name == table_name)
                    .cloned();

                // 当前数据库中当前表的信息
                let current_table_info = current_db_info
                    .table_infos
                    .iter()
                    .find_or_first(|a| a.table_name == table_name)
                    .cloned();

                // 如果两方有一方表缺失
                if (current_table_info.is_none() && cached_table_info.is_some())
                    || (current_table_info.is_some() && cached_table_info.is_none())
                {
                    // 将表缺失信息保存到excel中
                    add_excel_row(
                        db_name.clone(),
                        table_name.clone(),
                        "".into(),
                        "失败".into(),
                        "表缺失".into(),
                    );
                }
                // 当表都存在
                else if current_table_info.is_some() && cached_table_info.is_some() {
                    // 比较列信息
                    let cached_col_infos = cached_table_info.unwrap().col_infos;
                    let current_col_infos = current_table_info.unwrap().col_infos;

                    // 取列的 union
                    let mut union_col_names = HashSet::new();
                    for col_infos in [&cached_col_infos, &current_col_infos] {
                        for col_info in col_infos {
                            union_col_names.insert(col_info.col_name.clone());
                        }
                    }

                    // 循环检查每个列
                    for col_name in union_col_names {
                        // 分别基于列名称查找当前数据库中和缓存表中的列信息
                        let cached_col_info = cached_col_infos
                            .iter()
                            .find(|a| &a.col_name == &col_name)
                            .cloned();
                        let current_col_info = current_col_infos
                            .iter()
                            .find(|a| &a.col_name == &col_name)
                            .cloned();

                        // 如果当前列在缓存表中存在，但是在当前表中不存在或者反之
                        if (cached_col_info.is_some() && current_col_info.is_none())
                            || (cached_col_info.is_none() && current_col_info.is_some())
                        {
                            // 将列缺失信息保存到excel中
                            add_excel_row(
                                db_name.clone(),
                                table_name.clone(),
                                col_name.clone(),
                                "失败".into(),
                                "列缺失".into(),
                            );
                        }
                        // 如果当前列在缓存表和当前表中都存在，则检查列是否相同(比较类型)
                        else if cached_col_info.is_some() && current_col_info.is_some() {
                            // 读取列的类型信息
                            let cached_col_type = cached_col_info.unwrap().col_type;
                            let current_col_type = current_col_info.unwrap().col_type;

                            // 列一致
                            if cached_col_type == current_col_type {
                                add_excel_row(
                                    db_name.clone(),
                                    table_name.clone(),
                                    col_name.clone(),
                                    "成功".into(),
                                    "".into(),
                                );
                            } else {
                                // 列不一致
                                add_excel_row(
                                    db_name.clone(),
                                    table_name.clone(),
                                    col_name.clone(),
                                    "失败".into(),
                                    format!("列定义不一致{cached_col_type} --> {current_col_type}"),
                                );
                            }
                        }
                    }
                }
            }

            // 将文件保存到当路径
            wb.save(output_dir.clone()).unwrap();

            println!("output result file: {}", output_dir);
        }

        Err(e) => println!("数据文件读取错误: {e}"),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 读取启动参数
    let args = Args::parse();

    // 如果是创建本地缓存模式
    if args.create {
        // 如果是创建数据缓存信息，则默认为 48 mysql
        let db_host = if args.host.is_empty() {
            "localhost".into()
        } else {
            args.host
        };
        let db_port = if args.port == 0 { 3306 } else { args.port };
        let db_user = if args.user.is_empty() {
            "root".into()
        } else {
            args.user
        };
        let db_pass = if args.password.is_empty() {
            "123456".into()
        } else {
            args.password
        };
        let db_db = if args.database.is_empty() {
            "p10".into()
        } else {
            args.database
        };

        // 保存到的文件
        let save_to_file = if args.output_file.is_empty() {
            "dbInfo.bin".into()
        } else {
            args.output_file
        };

        // 编码密码
        let db_pass =  encode_str(db_pass.as_str());

        // 连接字符串
        let con_str = format!("mysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_db}");

        // 打印连接字符串
        println!("using connecting str: {}", con_str);

        // 创建连接池
        let pool = Pool::new(con_str.as_str())?;

        // 获取连接
        let mut con = pool.get_conn()?;

        // 创建本缓存
        create_db_info(&mut con, db_db.clone(), save_to_file.clone().into());

        // 输出完成信息
        println!("db info save @ {}", save_to_file);

        return Ok(());
    } else if
    // 如果是验证模式，默认使用本地连接
    args.validate {
        let db_host = if args.host.is_empty() {
            "localhost".into()
        } else {
            args.host
        };
        let db_port = if args.port == 0 { 3306 } else { args.port };
        let db_user = if args.user.is_empty() {
            "root".into()
        } else {
            args.user
        };
        let db_pass = if args.password.is_empty() {
            "123456".into()
        } else {
            args.password
        };
        let db_db = if args.database.is_empty() {
            "p10".into()
        } else {
            args.database
        };

        // 读取哈希的文件
        let input_file = if args.input_file.is_empty() {
            "dbInfo.bin".into()
        } else {
            args.input_file
        };

        // 保存到的文件
        let save_to_file = if args.output_file.is_empty() {
            "validateResult.xlsx".into()
        } else {
            // 如果指定了输出文件，验证输入文件名必须是 .xlsx结尾
            let reg = Regex::new(r"\.xlsx$")?;

            if reg.is_match(args.output_file.as_str()) {
                args.output_file
            } else {
                println!(
                    "指定输出文件格式不正确，必须以 .xlsx结尾，使用默认输出文件名 validateResult.xlsx"
                );
                "validateResult.xlsx".into()
            }
        };

        // 编码密码
        let db_pass =  encode_str(db_pass.as_str());

        // 连接字符串
        let con_str = format!("mysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_db}");

        // 打印连接字符串
        println!("using connecting str: {}", con_str);

        // 创建连接池
        let pool = Pool::new(con_str.as_str())?;

        // 获取可用连接
        let mut con = pool.get_conn()?;

        // 验证数据库信息
        validate_db_info(&mut con, db_db, input_file, save_to_file);
        return Ok(());
    }

    Ok(())
}
