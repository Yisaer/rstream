# How to use

## Create table

```sh
curl --request POST \
  --url http://127.0.0.1:3000/view \
  --data '{
  "sql": "create table t(a int, b int);"
}'
```

## Create view

```sh
curl --request POST \
  --url http://127.0.0.1:3000/view \
  --data '{
  "sql": "select a from t;"
}'
```

## Delete the view

```sh
curl --request DELETE \
  --url 'http://127.0.0.1:3000/view?id=0'
```
