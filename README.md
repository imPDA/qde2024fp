## Data Engineering School 2024 Final Project

Data pipeline for chemistry researchers.

### Main features
This project implements data pipeline with the following features:
- Selected tables from **ChEMBL database** transferred to separate DWH stage layer without modifications.
Transferring performs in parallel to speed up the process. Splitting by bunches allows to
save the progress in case of connection issues. Tables to transfer and bunch size can be
adjusted.
- **Morgan fingerprints calculation**. This is also done in bunches to parallel the
whole process and increase the speed, calculated fingerprints saved in S3 storage in
CSV format. If error occurred during calculation, unprocessed data will be saved separately and
another task will try to fix it. In case all error fixed, files containing unprocessed molecules
will be deleted, if not - they will persist in order to let user check it manually and decide
further actions. All parameters can be adjusted on DAG start - bunch size, source database
(you can use database ingested by previous feature or spin up your own database) and S3 destination.
- **Tanimoto similarity calculation**. Fingerprints calculated on previous step can be used to
calculate Tanimoto similarity between molecules from the whole dataset and some user
specified molecules (hereinafter referred to as the source molecules). Source molecules
read from S3 bucket from CSV files. Calculated Tanimoto similarity scores stored in S3 in
parquet format, whole similarity scores table per source molecule.
As before, different settings can be changed according to particular task.
- It can also **create a datamart** according to task provided:
  - Fact table with molecule similarities (filled from S3 files, created in previous step)
  - Dimension table with molecules and their properties (filled from stage layer only
  for molecules presented in fact table)
  - View with average similarity score per source molecule
  - View with average deviation of alogp of similar molecule from the source molecule
  - Pivoted view with similarity scores (1st column - source molecule, other columns - target molecules)
  - View which represents next most similar and next second most similar target molecule compared to
  the target molecule of the current row
  - View with average similarity score per:
    - Source_molecule
    - Source_molecule’s aromatic_rings and source_molecule’s heavy_atoms
    - Source_molecule’s heavy_atoms
    - Whole dataset
- In case of any error, alert via Telegram bot will be sent.

### How to set up
1. Copy the project
2. Set up environmental variables (please refer to `.env.example`)
3. Use `make up` command to setup containers with Airflow
4. Wait a bit
5. Navigate to `localhost:8080` and enter credentials from .env file
6. Set up required connections via Airflow interface<br>
    - Connection to remote RDB (refer to `remote_postgres` in DAGs)
    - Connection to S3 bucket (refer to `de-school-2024-aws-s3` in DAGs)
7. ⭐You may also want to adjust schedules according to particular task (as no
requirements for scheduling was provided - they left blank and all DAGs can be run
manually one by one)

### Examples
#### Stage layer
- `chembl_id_lookup` table
```
"chembl_id"	"entity_type"	"entity_id"	"status"	"last_active"
"CHEMBL1"	"COMPOUND"	517180	"ACTIVE"	34
"CHEMBL10"	"COMPOUND"	250	"ACTIVE"	34
"CHEMBL100"	"COMPOUND"	12356	"ACTIVE"	34
"CHEMBL1000"	"COMPOUND"	111185	"ACTIVE"	34
"CHEMBL10000"	"COMPOUND"	7079	"ACTIVE"	34
...
```
- `compound_properties` table
```
"molregno"	"mw_freebase"	"alogp"	"hba"	"hbd"	"psa"	"rtb"	"ro3_pass"	"num_ro5_violations"	"cx_most_apka"	"cx_most_bpka"	"cx_logp"	"cx_logd"	"molecular_species"	"full_mwt"	"aromatic_rings"	"heavy_atoms"	"qed_weighted"	"mw_monoisotopic"	"full_molformula"	"hba_lipinski"	"hbd_lipinski"	"num_lipinski_ro5_violations"	"np_likeness_score"
1	342	2	5	1	85	3	"N"	0	6		4	3	"ACID"	342	3	24	1	341	"C17H12ClN3O3"	6	1	0	-2
2	332	1	6	1	109	3	"N"	0	6		3	2	"ACID"	332	3	25	1	332	"C18H12N4O3"	7	1	0	-2
3	358	2	5	2	88	3	"N"	0	6		4	3	"ACID"	358	3	25	1	357	"C18H16ClN3O3"	6	2	0	-1
4	307	1	5	1	85	3	"N"	0	6		3	2	"ACID"	307	3	23	1	307	"C17H13N3O3"	6	1	0	-1
5	342	2	5	1	85	3	"N"	0	6		4	3	"ACID"	342	3	24	1	341	"C17H12ClN3O3"	6	1	0	-1
...
```
- `compound_structures` table
```
"molregno"	"molfile"	"standard_inchi"	"standard_inchi_key"	"canonical_smiles"
1	"RDKit..."	"InChI=1S/C17H12ClN3O3/c1-10-8-11(21-17(24)20-15(22)9-19-21)6-7-12(10)16(23)13-4-2-3-5-14(13)18/h2-9H,1H3,(H,20,22,24)"	"OWRSAHYFSSNENM-UHFFFAOYSA-N"	"Cc1cc(-n2ncc(=O)[nH]c2=O)ccc1C(=O)c1ccccc1Cl"
2	"RDKit..."	"InChI=1S/C18H12N4O3/c1-11-8-14(22-18(25)21-16(23)10-20-22)6-7-15(11)17(24)13-4-2-12(9-19)3-5-13/h2-8,10H,1H3,(H,21,23,25)"	"ZJYUMURGSZQFMH-UHFFFAOYSA-N"	"Cc1cc(-n2ncc(=O)[nH]c2=O)ccc1C(=O)c1ccc(C#N)cc1"
3	"RDKit..."	"InChI=1S/C18H16ClN3O3/c1-10-7-14(22-18(25)21-15(23)9-20-22)8-11(2)16(10)17(24)12-3-5-13(19)6-4-12/h3-9,17,24H,1-2H3,(H,21,23,25)"	"YOMWDCALSDWFSV-UHFFFAOYSA-N"	"Cc1cc(-n2ncc(=O)[nH]c2=O)cc(C)c1C(O)c1ccc(Cl)cc1"
4	"RDKit..."	"InChI=1S/C17H13N3O3/c1-11-2-4-12(5-3-11)16(22)13-6-8-14(9-7-13)20-17(23)19-15(21)10-18-20/h2-10H,1H3,(H,19,21,23)"	"PSOPUAQFGCRDIP-UHFFFAOYSA-N"	"Cc1ccc(C(=O)c2ccc(-n3ncc(=O)[nH]c3=O)cc2)cc1"
5	"RDKit..."	"InChI=1S/C17H12ClN3O3/c1-10-8-13(21-17(24)20-15(22)9-19-21)6-7-14(10)16(23)11-2-4-12(18)5-3-11/h2-9H,1H3,(H,20,22,24)"	"KEZNSCMBVRNOHO-UHFFFAOYSA-N"	"Cc1cc(-n2ncc(=O)[nH]c2=O)ccc1C(=O)c1ccc(Cl)cc1"
...
```
- `molecule_dictionary` table
```
"molregno"	"pref_name"	"chembl_id"	"max_phase"	"therapeutic_flag"	"dosed_ingredient"	"structure_type"	"chebi_par_id"	"molecule_type"	"first_approval"	"oral"	"parenteral"	"topical"	"black_box_warning"	"natural_product"	"first_in_class"	"chirality"	"prodrug"	"inorganic_flag"	"usan_year"	"availability_type"	"usan_stem"	"polymer_flag"	"usan_substem"	"usan_stem_definition"	"indication_class"	"withdrawn_flag"	"chemical_probe"	"orphan"
1		"CHEMBL6329"		0	0	"MOL"		"Small molecule"		0	0	0	0	0	-1	-1	-1	-1		-1		0				0	0	-1
2		"CHEMBL6328"		0	0	"MOL"		"Small molecule"		0	0	0	0	0	-1	-1	-1	-1		-1		0				0	0	-1
3		"CHEMBL265667"		0	0	"MOL"		"Small molecule"		0	0	0	0	0	-1	-1	-1	-1		-1		0				0	0	-1
4		"CHEMBL6362"		0	0	"MOL"		"Small molecule"		0	0	0	0	0	-1	-1	-1	-1		-1		0				0	0	-1
5		"CHEMBL267864"		0	0	"MOL"		"Small molecule"		0	0	0	0	0	-1	-1	-1	-1		-1		0				0	0	-1
...
```
#### S3 files
- Morgan fingerprints from `morgan_fingerprints_0.csv` from S3 bucket
```
molregno,morgan_fingerprints_base64
1,4P///wAIAAAtAAAAEkIIJBpdAFZIMhYaFtkABKrWYFIydhowpAA6MqgASFqeIBRurk0AGjAsCJQ0KFaScA==
2,4P///wAIAAAuAAAAEjAQCCQaKAkAVjYQSjLlAKoupj4ghnbyAG4qfkhaniAGAApM0IUAMCwIMGI0KFZISHA=
3,4P///wAIAAArAAAAAlIIQBkAFKxKMtkABIAoKqpgEHQqJCQUbG4AFlaqSFqiHBQ9ALRwTCwIlDQAJrkA
4,4P///wAIAAAjAAAAEkIIQC7+ApxKMuUAqtZg/vIAbqpIOQAIbLyFADAsCDBeAjQoVgT+
5,4P///wAIAAArAAAAEkIIJBoZACBWSEoy2QAEqtZgEHR28gAWVqpIWp4gCApM0E0AGjAsCDBiNChWknA=
...
```
#### Dimension and fact tables
- `molecule_properties`
```
"chembl_id"	"molecule_type"	"mw_freebase"	"alogp"	"psa"	"cx_logp"	"molecular_species"	"full_mwt"	"aromatic_rings"	"heavy_atoms"
"CHEMBL102246"	"Small molecule"	176	2	16	2	"NEUTRAL"	176	1	13
"CHEMBL1077122"	"Small molecule"	881	5	156	3	"NEUTRAL"	881	0	63
"CHEMBL1097210"	"Small molecule"	897	5	159	3	"NEUTRAL"	897	0	64
"CHEMBL112998"	"Small molecule"	436	2	111	2	"NEUTRAL"	436	3	29
"CHEMBL1132"	"Small molecule"	148	2	25	1	"BASE"	148	1	11
...
```
- `molecule_similarities`
```
"id"	"source_molecule"	"target_molecule"	"similarity_score"	"has_duplicates_of_last_largest_score"
1	"CHEMBL263076"	"CHEMBL263076"	1	false
2	"CHEMBL266458"	"CHEMBL266458"	1	true
3	"CHEMBL263076"	"CHEMBL6338"	0.7058823529411765	false
4	"CHEMBL263076"	"CHEMBL6364"	0.6571428571428571	false
5	"CHEMBL263076"	"CHEMBL6336"	0.6341463414634146	false
...
```
#### Views
- average similarity per source molecule
```
"source_molecule"	"avg_similarity"
"CHEMBL6236"	0.7443962642923609
"CHEMBL6343"	0.7681424048588227
"CHEMBL6218"	0.7345664297929055
"CHEMBL447227"	0.7131266391154452
"CHEMBL6313"	0.6470086202239022
...
```
- average deviation of alogp
```
"source_molecule"	"avg_alogp_deviation"
"CHEMBL6236"	0.60000000000000000000
"CHEMBL6343"	0.10000000000000000000
"CHEMBL6218"	0.80000000000000000000
"CHEMBL447227"	1.4000000000000000
"CHEMBL6313"	1.7000000000000000
...
```
- average similarity pivoted
```
"source_molecule"	"CHEMBL1077122"	"CHEMBL1097210"	"CHEMBL112998"	"CHEMBL1214355"	"CHEMBL12700"	"CHEMBL1335176"	"CHEMBL1365136"	"CHEMBL1408358"	"CHEMBL1460053"	"CHEMBL1497434"	"CHEMBL182716"	"CHEMBL184070"	"CHEMBL185146"	"CHEMBL185757"	"CHEMBL185959"	"CHEMBL186554"	"CHEMBL187064"	"CHEMBL1882364"	"CHEMBL189251"	"CHEMBL197251"	"CHEMBL204146"	"CHEMBL216458"	"CHEMBL2206411"	"CHEMBL258254"	"CHEMBL263076"	"CHEMBL263077"	"CHEMBL266223"	"CHEMBL266457"	"CHEMBL266458"	"CHEMBL266484"	"CHEMBL266885"	"CHEMBL266886"	"CHEMBL267357"	"CHEMBL267864"	"CHEMBL268097"	"CHEMBL268150"	"CHEMBL268365"	"CHEMBL268528"	"CHEMBL268556"	"CHEMBL268596"	"CHEMBL268770"	"CHEMBL269134"	"CHEMBL269340"	"CHEMBL270107"	"CHEMBL275768"	"CHEMBL291551"	"CHEMBL3040295"	"CHEMBL3092045"	"CHEMBL3236668"	"CHEMBL332848"	"CHEMBL360624"	"CHEMBL362885"	"CHEMBL366289"	"CHEMBL375809"	"CHEMBL380444"	"CHEMBL413858"	"CHEMBL415285"	"CHEMBL4162761"	"CHEMBL4170029"	"CHEMBL4171119"	"CHEMBL4171516"	"CHEMBL446445"	"CHEMBL446850"	"CHEMBL4754666"	"CHEMBL4757701"	"CHEMBL477921"	"CHEMBL4782528"	"CHEMBL4788996"	"CHEMBL6207"	"CHEMBL6214"	"CHEMBL6217"	"CHEMBL6219"	"CHEMBL6222"	"CHEMBL6236"	"CHEMBL6238"	"CHEMBL6239"	"CHEMBL6240"	"CHEMBL6243"	"CHEMBL6266"	"CHEMBL6272"	"CHEMBL6308"	"CHEMBL6328"	"CHEMBL6329"	"CHEMBL6336"	"CHEMBL6338"	"CHEMBL6344"	"CHEMBL6349"	"CHEMBL6352"	"CHEMBL6359"	"CHEMBL6362"	"CHEMBL6363"	"CHEMBL6364"	"CHEMBL6365"
"CHEMBL112998"			1									0.6779661016949152		0.7166666666666667	0.6721311475409836	0.6779661016949152	0.6721311475409836		0.6666666666666666																																	0.6721311475409836	0.6666666666666666				0.6779661016949152
"CHEMBL216458"	0.8709677419354839	0.8736842105263158				0.8586956521739131	0.8217821782178217	0.8736842105263158	0.8736842105263158	0.797979797979798								0.7830188679245284				1																																		1
"CHEMBL263076"																					0.6216216216216216		0.5476190476190477		1																0.525							0.5476190476190477																		0.5142857142857142																		0.6341463414634146	0.7058823529411765		0.6052631578947368					0.6571428571428571
"CHEMBL266223"																										0.6301369863013698	1																															0.6716417910447762	0.6617647058823529	0.6338028169014085	0.6				0.6176470588235294		0.6470588235294118	0.5897435897435898				0.6973684210526315
"CHEMBL266457"																												1						0.6545454545454545	0.660377358490566											0.6326530612244898																								0.7272727272727273			0.7647058823529411			0.6538461538461539	0.7307692307692307									0.631578947368421		0.6379310344827587
...
```
- next similar and next second similar
```
"source_molecule"	"target_molecule"	"similarity_score"	"1st_next_chembl_id"	"2nd_next_chembl_id"
"CHEMBL112998"	"CHEMBL112998"	1	"CHEMBL185757"	"CHEMBL415285"
"CHEMBL112998"	"CHEMBL185757"	0.7166666666666667	"CHEMBL415285"	"CHEMBL186554"
"CHEMBL112998"	"CHEMBL415285"	0.6779661016949152	"CHEMBL186554"	"CHEMBL184070"
"CHEMBL112998"	"CHEMBL186554"	0.6779661016949152	"CHEMBL184070"	"CHEMBL185959"
"CHEMBL112998"	"CHEMBL184070"	0.6779661016949152	"CHEMBL185959"	"CHEMBL362885"
"CHEMBL112998"	"CHEMBL185959"	0.6721311475409836	"CHEMBL362885"	"CHEMBL187064"
"CHEMBL112998"	"CHEMBL362885"	0.6721311475409836	"CHEMBL187064"	"CHEMBL366289"
"CHEMBL112998"	"CHEMBL187064"	0.6721311475409836	"CHEMBL366289"	"CHEMBL189251"
"CHEMBL112998"	"CHEMBL366289"	0.6666666666666666	"CHEMBL189251"
"CHEMBL112998"	"CHEMBL189251"	0.6666666666666666
...
```
- average similarity per different groupings
```
"source_molecule"	"aromatic_rings"	"heavy_atoms"	"avg"
"TOTAL"	"TOTAL"	"TOTAL"	0.7318814130148465
"CHEMBL6236"			0.7443962642923609
"CHEMBL6343"			0.7681424048588227
"CHEMBL6218"			0.7345664297929055
"CHEMBL447227"			0.7131266391154452
...
```
