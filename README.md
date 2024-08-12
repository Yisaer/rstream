# How to use

## Create table

```sh
curl --request POST \
  --url http://127.0.0.1:3000/view \
  --data '{
  "sql": "create table t(a int, b int,c int,d int);"
}'
```

## Create view

```sh
curl --request POST \
  --url http://127.0.0.1:3000/view \
  --data '{
  "sql": "select a as e from t where b > 30;"
}'
```

send/recv data by mqtt broker 127.0.0.1:1883

send data topic: /yisa/data

recv data topic: /yisa/data2

## Delete the view

```sh
curl --request DELETE \
  --url 'http://127.0.0.1:3000/view?id=0'
```
