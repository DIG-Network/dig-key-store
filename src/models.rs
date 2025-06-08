use diesel::prelude::*;
use crate::schema::cache;

#[derive(Queryable, Debug)]
pub struct CacheEntry {
    pub cache_key: String,
    pub cache_value: Vec<u8>,
    pub expires: Option<i64>,
    pub last_accessed: i64,
}

#[derive(Insertable, Debug)]
#[diesel(table_name = cache)]
pub struct NewCacheEntry<'a> {
    pub cache_key: &'a str,
    pub cache_value: &'a [u8],
    pub expires: Option<&'a i64>,
    pub last_accessed: &'a i64,
}