# Matcher

`Matcher` is used in filter and router. Existing matchers list here. 

```toml
match-schema = "test"
match-table = "test_table_*"
match-table = ["a*", "b*"]
match-dml-op = "delete" # rejects ddl
match-dml-op = ["insert", "update", "delete"] 
match-ddl-regex = '(?i)^DROP\sTABLE' # rejects dml

