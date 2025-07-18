# Утилита экспортер

Создана для экспорта большого числа логов. 

## Флаги

--hosts='IP1,IP2,IP3' список адресов seq-proxy

--out=\<dir\> путь к директории в которой будут создаватся файлы с экспортированными данными

--from=2006-01-02T15:04:05Z07:00 начало интервала экспорта

--to=2006-01-02T15:04:05Z07:00 конец интервала экспорта

--query='\<query\>' строка с поисковым запросом

--window=\<num\> интервал на который бьется запрос

Утилита работает следующим образом. Разбивает заданный интервал [from;to] на подинтервалы размером window:

 - [to-window;to]
 - [to-2*window;to-1*window)
 - [to-3*window;to-2*window)
 - ...
 - [from;to-N*window)

В директории out будут созданы файлы. В каждом файле результат запроса в подинтервале. В имени файла будет начало интервала.

## Вспомагательные скрипты

 - hosts.sh - возвращает список адресов seq-proxy (надо внести изменение в скрипт что бы выбрать нужный кластер) 
 - cat.sh - выводит (или склеивает, если перенаправить в файл) содержимое файликов в директории в порядке от самого нового к самому старом

## Пример использования 
```bash
./exporter \
    --hosts `./cmd/exporter/hosts.sh` \ # получаем список хостов
    --out './datatest/' \
    --from '2023-08-29T15:56:55+03:00' \ # указываем зону +03:00 если имеем ввиду время по Msk
    --to '2023-08-29T15:59:27+03:00' \
    --query 'service:"warden" AND message:"open subscription"' | tee ./exporter.log

./cmd/exporter/cat.sh ./datatest | head | jq . | grep "ts" # проверяем время документов в начале датасета

./cmd/exporter/cat.sh ./datatest | tail | jq . | grep "ts" # проверяем время документов в конце датасета

./cmd/exporter/cat.sh ./datatest | gzip > joined.json.gz # склеиваем файлы в один

```





