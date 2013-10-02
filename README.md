# API ready package


 _Russian_ (English below)

## О проекте

APIReady - это модуль для Pythob для быстрого создания API на основе CSV файлов


## Установка

Для установки необходимы библиотеки

	sudo pip install dateutil
	sudo pip install pymongo
	sudo pip install tornadoweb	

## Использование

### Анализ 

Для анализа используется скрипт apireader.py принимающий следующие параметры:

* --format (-f) - формат входящего файла (сейчас CSV)
* --source (-s) - источник, ссылка на файл
* --delimiter (-d) - символ разделитель полей в CSV файле
* --config (-c) - файл для сохранения конфигурации
* --dictshare (-i) - уровень "чувствительности" к данным в словаре. Доля уникальных записей к общему числу по которой делается предположение что это словарь.
* --update (-u) - обновить конфигурацию (не перезаписывать поля version и app_key)

**Примеры**

_Разбор данных_
Преобразование файла с данными по российским послам "allamb.tsv" разделенного табуляцией в файл конфигурации "amb.config".

	python apireader.py -f csv -s data/allamb.tsv -d '\t' -c "amb.config" analyze 
	


Преобразование файла с данными по телефонам МЧС России "phones.tsv" разделенного запятыми в файл конфигурации "mchsphones.config".

	apireader.py -f csv -s data/phones.tsv -d ',' -c 'mchsphones.config' analyze
	
	
__

### Подготовка 

При подготовке данных в базу загружаются сами данные и словари полученные на их основе.


	apireader.py -c 'mchsphones.config' prepare

### Публикация

По команде "serve" стартует веб-сервер tornado который обслуживает все запросы:

	apireader.py -c 'mchsphones.config' serve


## Точек входа

API для доступа идет по следующим ссылкам

	/[app_key]/info  - базовая информация о массиве
	/[app_key]/list  - список всех записей
	/[app_key]/query - запрос в базу
	/[app_key]/dicts/[dict_key] - все записи словаря
	/[app_key]/key/[uniq_key] - пермалинк на конкретную запись

**Пример**
 CSV файл посло с app_key = 'amb' и полями словарей 'firstname', 'rank', 'surname'

	/amb/info
	/amb/list
	/amb/query
	/amb/dicts/firstname - словарь имен
	/amb/dicts/surname  - словарь фамилий




## Описание файла конфигурации

Файлы конфигурации - это JSON файлы в определеной структуре.

* **app_key** - уникальный ключ массива данных под которым он доступен
* **count** - общее число записей в массиве данных
* **num_fields** - число полей в массиве данных
* **delimiter** - разделяющий символ (для CSV)
* **fieldtypes** - список полей в массиве данных
* **dictkeys** - список полей на основе которых формируются словари
* **format** - формат файла массива данных
* **source** - источний информации (ссылка на файл)
* **version** - версия массива данных - используется в случаях когда может публиковаться несколько версий.



Пример конфигурационного файла:

	{'app_key': 'allamb',
	'count': 571,
 	'delimiter': '\\t',
 	'fieldtypes': {'age': 'int',
				'birthday': 'str',
				'department': 'str',
				'depjoindate': 'str',
				'ethnics': 'str',
				'firstname': 'str',
				'gender': 'str',
				'midname': 'str',
				'name': 'str',
				'position': 'str',
				'rank': 'str',
				'rankage': 'int',
				'rankdate': 'str',
				'surname': 'str'},
 	'format': 'csv',
 	'num_fields': 14,
	 'source': 'data/allamb.tsv',
 	'version': None}


_English_ (Russian above)

## About project

APIReady is python script and module for quick API creation. It uses provided data source like CSV file and builds REST+JSON API based on data provided.

## Installation

You need to install following packages:

	sudo pip install dateutil
	sudo pip install pymongo
	sudo pip install tornadoweb	

## Usage

### Analysis

### Preparation

### Running
