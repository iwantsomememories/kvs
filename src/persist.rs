use std::ops::Range;

use serde::{Serialize, Deserialize};

/// 保存在磁盘上的操作
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum Operation {
    /// 设置键值对
    Set { 
        /// 键
        key: String, 
        /// 值
        value: String 
    },
    /// 删除键
    Rm { 
        /// 键
        key: String 
    },
}

/// 记录操作在log文件中的位置及长度
pub struct OperationPos {
    gen: u64,
    pos: u64,
    len: u64
}

impl From<(u64, Range<u64>)> for OperationPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        OperationPos { gen: gen, pos: range.start, len: range.end - range.start }
    }
}