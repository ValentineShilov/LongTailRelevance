Шилов Валентин

Инструкция к решению конкурса по предсказанию релевантности по пользовательскому поведению
1. Вычисление кликовых статистик для документа по всем запросам
	Структура папок:
		stage1.py - вычисление фичей 1-й этап
		stage2.py - вычисление фичей 2-й этап

		code/url.data
		code/finalRank2.ipynb
		code/sample.csv
		code/queries.tsv
		code/train.marks.tsv

		data/2017/part-m-*.bz2  - исходные данные
		hadoop/reducer.py  \
		hadoop/mapper.py  -- поиск запросов, которые есть в queries.tsv
		hadoop/sample.csv
		hadoop/url.data
		hadoop/queries.tsv
		hadoop/run_streaming.sh

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

4. открыть code/finalRank2.ipynb, обучить catboost и получить результат



P. S. топ фичей:
значение префиксов: doc - только для документа, dom - для домена, q - запрос+док

doc_np.log(1.0 + self.nshows ) 0.0018246727223626413  - странно, важность большая, но если фичу убрать, то скор почти не меняется
doc_nlast 0.0009434471303183534 - сколько кликнут последним
dom_nafter 0.0007417996431634544 - среднее число кликов на предыдущие док
doc_nafter 0.000613047204941708 - среднее число кликов на последующие док
q_ctr 0.0005477373760451876		- CTR для пары запрос - документ
q_qdist1 0.0004968315600831774   - расстояние от запроса из логов до интересующего нас запроса для пары запрос-документ, в данном случае не имеет смысла, работает как флаг, что в логах есть совпадающий запрос
dom_qdist1 0.0004255723886392726 - минимальное расстояние от запроса из логов до интересующего нас запроса для пары документа
q_pos_clicks@2 0.00037513763619512197 - CTR на второй позиции для запрос-документ
dom_clicks_pos@9 0.0003627502109559977 - CTR на 9 позиции для домена
dom_nbefore 0.0003391219161640313
dom_clicks_pos@2 0.00030511237177610617
doc_pos_clicks@0 0.0002807313250726695
dom_qdist2 0.00027960300043905484
dom_ctr 0.00027174932495999116
dom_pos_clicks@12 0.00026913442880616145
dom_pos_clicks@14 0.00025861583079100914
q_nlast 0.0002525877336607696
dom_clicks_pos@3 0.0002487706205543949
dom_nlast 0.0002479546180924608
dom_np.log(1.0 + self.nclicks) 0.00023751275532368776
doc_clicks_pos@9 0.00023644436328018603
doc_pos_clicks@1 0.00023608874675917768
doc_qdist1 0.0002343311236550072
doc_clicks_pos@4 0.00020881451278409457
doc_clicks_pos@3 0.00020742708982246505
dom_clicks_pos@14 0.00019666046122324143
dom_pos_clicks@11 0.00019174453029779936
doc_pos_clicks@11 0.00019169495939563852
doc_nbefore 0.0001895750185417766
doc_clicks_pos@5 0.00017703037762994978
dom_pos_clicks@9 0.00017523928350759999
dom_clicks_pos@7 0.00017067549438753993
dom_np.log(1.0 + self.nshows) 0.0001658270163693576
doc_pos_clicks@3 0.00015539489552696129
dom_clicks_pos@8 0.00015090443844445467
dom_clicks_pos@15 0.00014593552209329896
q_pos_clicks@1 0.00014068158049573842
dom_clicks_pos@1 0.0001397789449831821
dom_clicks_pos@12 0.00013124709415301972
dom_pos_clicks@0 0.00011941895189970797
dom_clicks_pos@13 0.00011494031111469738
doc_pos_clicks@9 9.474208359561143e-05
dom_clicks_pos@6 9.307438773287213e-05
q_clicks_pos@8 9.094478251336913e-05
dom_pos_clicks@15 8.674931395502572e-05
dom__avg_time_cnt 8.288584914439845e-05
doc_ctr 7.575842878027039e-05
dom_clicks_pos@4 7.393460451099454e-05
doc_pos_clicks@5 6.743661414088109e-05
q_clicks_pos@9 6.719148849942957e-05
