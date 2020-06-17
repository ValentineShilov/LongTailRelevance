
Инструкция к решению конкурса по предсказанию релевантности
1. Вычисление кликовых статистик для документа по всем запросам
	Структура папок:
		stage1.py - вычисление фичей 1-й этап
		stage2.py - вычисление фичей 2-й этап

	shell:

	mkdir res15
	pip install python-Levenshtein
	#also install other libraries imported in 
	#stage1.py, stage2.py if not installed by default
	#will take a lot of time:
	python3 stage1.py data/2017/ res15/ code/
	python3 stage2.py data/2017/ res15/ code/ features_15.txt

2. Вычисление кликовых статистик для хоста по всем запросам
	mkdir res15_domain
	python3 stage1_domain.py data/2017/ res15_domain/ code/
	python3 stage2.py data/2017/ res15/ code/ features_15_domain.txt

3. Поиск совпадающих запросов в логах:
	я делал это на hadoop
	1) поместить папку hadoop на кластер
	2) отредактировать в run_streaming.sh OUTDIR=... и INFILES=... 
		 OUTDIR - папка для результата
		 INFILES - путь к файлам с логами (part-*.bz2) в hdfs
	3) запустить run_streaming.sh
	4) переместить в локальную фс и упаковать 
		hdfs dfs -cat $OUTDIR/part-00000 >found_queries.txt
		bzip2 found_queries.txt
	5) переместить результат на локальный компьютер в папку found_queries
	6)  mkdir res15_fq
		python3 stage1.py found_queries/ res15_fq/ code/
		mv res15_fq/found_queries.txt.bz2.txt code/
4. открыть finalranker.ipynb, обучить catboost и получить результат
5. Извлечь топ1 доков
6. подсчитать tfidf, bm25, USE  - см. папку USE_tfidf
7. открыть finalranker.ipynb, обучить catboost, lightgbm и получить результат
