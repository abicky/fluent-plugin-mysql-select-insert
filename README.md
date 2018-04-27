# fluent-plugin-mysql-select-insert

[Fluentd](https://fluentd.org/) output plugin to insert records by SELECT query.
You can select records using events data and join multiple tables.

## Installation

### RubyGems

```
$ gem install fluent-plugin-mysql-select-insert
```

### Bundler

Add following line to your Gemfile:

```ruby
gem "fluent-plugin-mysql-select-insert"
```

And then execute:

```
$ bundle
```

## Configuration

Parameter | Description    | Default
----------|----------------|-------------
host      | Database host  | 127.0.0.1
port      | Database port  | 3306
database  | Database name  | (required)
username  | Database user  | (required)
password  | Database password | (empty string)
table     | Table into which records are inserted | (required)
select_query | SELECT query without WHERE clause to insert records | (required)
condition_column | Column used in WHERE clause to compare with values of 'condition_key' in events | (required)
condition_key | Key whose value is used in WHERE clause to compare with 'condition_column' | (required)
extra_condition | Extra condition used in WHERE clause. You can specify placeholders like 'col1 = ? AND col2 = ?, ${tag[0]}, ${tag[1]}'. The metadata is extracted in the second or following arguments. | (empty array)
ignore    | Add 'IGNORE' modifier to INSERT statement | false

## Example Configuration

```
<match pattern>
  @type mysql_select_insert
  database fluent_plugin_mysql_select_insert
  username root

  table accessed_users

  select_query "SELECT
      users.id
    FROM
      users
      INNER JOIN
        devices
      ON
        devices.id = users.device_id"

  condition_key uuid
  condition_column devices.uuid
  extra_condition "app_id = ?, ${app_id}"

  ignore true

  <buffer app_id>
  </buffer>
</match>
```

Assuming that the following data comes in:

```
pattern {"uuid":"03449258-29ce-403c-900a-a2c6ea1d09a2","app_id":1}
pattern {"uuid":"11aebc82-b661-44ab-951d-d1618058699a","app_id":1}
```

The INSERT statement will be like below:

```
INSERT IGNORE INTO
  accessed_users
SELECT
  users.id
FROM
  users
  INNER JOIN
    devices
  ON
    devices.id = users.device_id
WHERE
  uuid IN (
    '03449258-29ce-403c-900a-a2c6ea1d09a2',
    '11aebc82-b661-44ab-951d-d1618058699a'
  )
  AND app_id = 1
;
```


## Copyright

* Copyright(c) 2018- abicky
* License
  * Apache License, Version 2.0
