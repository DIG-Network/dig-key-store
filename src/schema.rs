diesel::table! {
    cache (cache_key) {
        cache_key -> Text,
        cache_value -> Binary,
        expires -> Nullable<BigInt>,
        last_accessed -> BigInt,
    }
}
