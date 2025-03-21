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


