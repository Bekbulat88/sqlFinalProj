create database customers;

drop table customer;

CREATE TABLE customer (
    Id_client INT,
    total_amount INT,
    Gender VARCHAR(7),
    Age INT NULL,
    Count_city INT,
    Response_communication INT,
    Communcation_3month INT,
    Tenure INT
);

drop table transactions;

CREATE TABLE transactions (
    date_new DATE,
    id_check INT,
    id_client INT,
    Count_products DECIMAL(6 , 3 ),
    Sum_payment DECIMAL(6 , 2 )
);

SHOW VARIABLES LIKE 'secure_file_priv';

#так как wizard не загружает null значения грузим данные через load data
LOAD DATA INFILE "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/customers.csv"
INTO TABLE customer
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(Id_client,
total_amount,
@Gender,
@Age,
Count_city,
Response_communication,
Communcation_3month,
Tenure)
SET Gender = NULLIF(@Gender, ''),
	Age = NULLIF(@Age, '');

/*
1. список клиентов с непрерывной историей за год, то есть каждый месяц на регулярной основе без пропусков за указанный годовой период,
 средний чек за период с 01.06.2015 по 01.06.2016,
 средняя сумма покупок за месяц, 
 количество всех операций по клиенту за период;
*/

#для того чтобы все скомпановать в одном запросе, использую 3 CTE'шки и в каждой держу поле с клиентом для JOIN с основным запросом
#первая CTE вытаскиваю клиента, чек и сумму чека
WITH check_sum AS (
	SELECT id_client, 
		   id_check, 
           SUM(sum_payment) as sum
	FROM transactions
	GROUP BY id_client, id_check
), sum_by_month AS (   # вторая CTE вытаскиваю клиента, месяц и сумму за месяц
	SELECT id_client, 
		   MONTH(date_new) AS month, 
           SUM(Sum_payment) AS sum_by_month
	FROM transactions 
	GROUP BY id_client, MONTH(date_new)
), transactions_count AS ( # так как количество операций, это количество чеков то вытаскиваю клиента и количество чеков
	SELECT t.id_client, 
		   COUNT(id_check) AS transactions_count
	FROM transactions AS t
	GROUP BY t.id_client
)
SELECT c.id_client,
	   AVG(sum)  AS average_check,
       AVG(sbm.sum_by_month) as average_by_month,
	   transactions_count
FROM customer AS c		
JOIN transactions AS t
	ON t.id_client = c.id_client
JOIN sum_by_month AS sbm
	ON sbm.id_client = c.id_client
JOIN check_sum AS cs
	ON cs.id_client = c.id_client
JOIN transactions_count
	ON transactions_count.Id_client = c.Id_client
WHERE Tenure = 12
GROUP BY c.id_client;


/*
информацию в разрезе месяцев:
a)средняя сумма чека в месяц;
b)среднее количество операций в месяц;
c)среднее количество клиентов, которые совершали операции;
d)долю от общего количества операций за год и долю в месяц от общей суммы операций;
e)вывести % соотношение M/F/NA в каждом месяце с их долей затрат;

*/
#!!! 1 операция = 1 чек

#средняя сумма чека
WITH check_sum AS (  #сумма чека 
SELECT MONTH(t.date_new) AS month, 
	   t.id_check, 
       SUM(t.Sum_payment) AS sum
FROM transactions AS t
GROUP BY MONTH(t.date_new), t.id_check
), 
	check_count AS ( #количество операций
SELECT MONTH(t.date_new) AS month,
	   t.id_check,
	   COUNT(t.id_check) AS check_count
FROM transactions AS t
GROUP BY MONTH(t.date_new), t.id_check
), 
	client_count as (  #количесвто клиентов
SELECT MONTH(date_new) as month,
	   COUNT(distinct id_client) as count
FROM transactions
GROUP BY month(date_new)
)
SELECT 
	MONTH(t.date_new) AS month, 
	AVG(cs.sum) AS avg_sum_by_check,
	AVG(cc.check_count) AS avg_check,
	(SELECT AVG(count) FROM client_count) AS avg_client,
	ROUND(COUNT(distinct t.id_check) * 100 / (SELECT Count(distinct id_check) FROM transactions), 2) AS check_percentage, #процент чеков в месяц от общего количества
	ROUND(SUM(t.Sum_payment) * 100 / (SELECT SUM(Sum_payment) FROM transactions), 2) AS payment_percentage #процент суммы в месяц от общей суммы
FROM transactions AS t
JOIN check_sum AS cs
	 ON t.id_check = cs.id_check
JOIN check_count AS cc
	 ON t.id_check = cc.id_check
GROUP BY month
ORDER BY month;


#вывести % соотношение M/F/NA в каждом месяце с их долей затрат;

WITH monthly_by_gender AS (
    SELECT 
        MONTH(t.date_new) AS month,
        c.Gender,
        COUNT(t.Id_client) AS client_count,
        SUM(t.Sum_payment) AS total_spending
    FROM 
        transactions AS t
    JOIN 
        customer AS c ON t.id_client = c.Id_client
    GROUP BY 
        month, c.Gender
),  total_clients_spend AS (
    SELECT
        month,
        SUM(client_count) AS total_clients,
        SUM(total_spending) AS total_spending_month
    FROM 
        monthly_by_gender
    GROUP BY 
        month
)
SELECT
    m.month,
    m.Gender,
    (m.client_count / tlc.total_clients) * 100 AS gender_percentage,
    (m.total_spending / tlc.total_spending_month) * 100 AS spending_share
FROM 
    monthly_by_gender AS m
JOIN 
    total_clients_spend AS tlc ON m.month = tlc.month
ORDER BY 
    m.month, m.Gender;



/*
возрастные группы клиентов с шагом 10 лет и отдельно клиентов, 
у которых нет данной информации, с параметрами сумма и количество операций за весь период, 
и поквартально - средние показатели и %.
*/

with age_gradation as (
SELECT 
	distinct id_client,
	CASE 
     WHEN age between 0 AND 9 THEN 'Under 10'
     WHEN age between 10 AND 19 THEN '10-19'
     WHEN age between 20 AND 29 THEN '20-29'
     WHEN age between 30 AND 39 THEN '30-39'
     WHEN age between 40 AND 49 THEN '40-49'
     WHEN age between 50 AND 59 THEN '50-59'
     WHEN age between 60 AND 69 THEN '60-69'
     WHEN age between 70 AND 79 THEN '70-79'
     WHEN age between 80 AND 89 THEN '80-89'
     ELSE 'No info'
	END as age
FROM 
	customer AS c
) , sumAndCount as (
SELECT 
	t.date_new as date,
	age,
	SUM(sum_payment) as sum,
	COUNT(distinct id_check) as cnt,
    quarter(t.date_new) as quart
FROM age_gradation as ag
JOIN transactions as t
ON ag.id_client = t.id_client
GROUP by  age, date_new
) select 	age, 
			sum(sum) as totalPayments, 
			sum(cnt) as totalCheckCount,
            quart,
            avg(sum(sum)) over (partition by quart) average_quart_payment,
            avg(sum(cnt)) over (partition by quart) as average_quart_operations
		from sumandcount
        group by age, quart