# SPARK-SUBMIT

Должно заменить плагин BigDataTools для IDEA, по-идее.


## Как собрать проект?

```shell
python -m build
```

## Как иcпользовать?


1. Нужно создать hocon-файл. Пример см в tests/configs/spark-submit.conf

2. Установть пакет ???
```shell
pip install .\ssh_spark_submit-0.1.0-py3-none-any.whl
```

3. Запустить пакет 

```shell
python .\dist\spark_submit-0.0.1-py3-none-any.whl --conf tests/configs/spark-submit.conf
```

    