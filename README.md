# SPARK-SUBMIT

Должно заменить плагин для IDEA, по-идее.


Собрать проект 
```shell
python -m build
```


1. Нужно создать hocon-файл. Пример см в tests/spark-submit.txt

2. Установть пакет
```shell
pip install .\ssh_spark_submit-0.1.0-py3-none-any.whl
```

3. Запустить пакет 

```shell
python .\dist\spark_submit-0.0.1-py3-none-any.whl\spark-submit /path/to/filename.ini
```

    