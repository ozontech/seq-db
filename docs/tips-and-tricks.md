# Tips and tricks

## Store

### Get from and to borders of fractions

```bash
kubectl logs seq-db-ssd-z23-2 | grep “created from active” | awk '{print $1" "$9" "$10}'
```

```text
2021-12-14T20:54:44.245Z from=1635233607521, to=1639515260288,
2021-12-14T20:55:45.940Z from=1639508728103, to=1639515321825,
2021-12-14T20:56:45.270Z from=1635233370984, to=1639515382108,
2021-12-14T20:57:49.270Z from=1639508481462, to=1639515443339,
2021-12-14T20:58:45.016Z from=1639508498468, to=1639515500604,
```
