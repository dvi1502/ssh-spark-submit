# SPARK-SUBMIT

Должно заменить плагин BigDataTools для IDEA, по-идее.

## Как собрать проект?

```shell
python -m build
```

## Как иcпользовать?

1. Нужно создать hocon-файл. Пример см. в tests/configs/spark-submit.conf

2. Собрать пакет из исходных файлов проекта
```shell
python setup.py bdist_wheel
```

3. Как установть пакет в целевое окружение ??? 

```shell
cd ./.run
python -m venv venv
.\venv\Scripts\activate   
python.exe -m pip install --upgrade pip
pip install <ANY_PATH>\ssh_spark_submit-0.1.0-py3-none-any.whl
```

## Варианты испрользования

1) Создать конфигурационный файл для проекта

```shell
python -m ssh_spark_submit --new --project C:\Users\DmVIvakin\sources\domain-technica-sms\smsc-consumer
```

2) Проверить конфигурационный файл

```shell
python -m ssh_spark_submit --show --conf spark-submit.conf
```

3) Запустить пакет

```shell
python -m ssh_spark_submit --run --conf spark-submit.conf
```

4) Опубликовать пакет

```shell
python -m ssh_spark_submit --deploy --conf spark-submit.conf
```

## Удалить пакет
```shell
pip uninstall -y ssh_spark_submit
```