/* Creating the synonyms for the OLTP table from Yang */
CREATE SYNONYM th FOR ops$yyang00.theatre;
CREATE SYNONYM pr FOR ops$yyang00.production;
CREATE SYNONYM pf FOR ops$yyang00.performance;
CREATE SYNONYM cl FOR ops$yyang00.client;
CREATE SYNONYM tkt_p FOR ops$yyang00.ticketpurchase;

/* Getting detailed idea of the tables */
SELECT*FROM th;
SELECT*FROM pr;
SELECT*FROM pf;
SELECT*FROM cl;
SELECT*FROM tkt_p;

/* CREATING SEQUENCE FOR SURROGATE KEYS IN DATA MART */
/* CREATING DIMENSION  TABLES AND FACT TABLE FOR DATA MART */ 
CREATE SEQUENCE dth_seq
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;

CREATE TABLE Group2_Dimth(
    Theatre_id NUMBER(5) PRIMARY KEY,
    Theatre# NUMBER(5) NOT NULL UNIQUE,
    Name VARCHAR2(20) NOT NULL,
    Street VARCHAR2(20),
    Town VARCHAR2(20),
    County VARCHAR2(20),
    MainTel CHAR(11)
);


CREATE SEQUENCE dpr_seq
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;

CREATE TABLE Group2_Dimpr(
    P_id NUMBER(5) PRIMARY KEY,
    P# NUMBER(5) NOT NULL UNIQUE,
    Title VARCHAR2(20) NOT NULL,
    ProductionDirector VARCHAR2(20),
    PlayAuthor VARCHAR2(20)
);

CREATE SEQUENCE dcl_seq
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;

CREATE TABLE Group2_Dimcl(
    Client_id NUMBER(5) PRIMARY KEY,
    Client# NUMBER(5) NOT NULL UNIQUE,
    Title VARCHAR2(10) NOT NULL,
    Name VARCHAR2(30),
    Street VARCHAR2(20),
    Town VARCHAR2(20),
    County VARCHAR2(20),
    TelNo CHAR(11),
    email VARCHAR2(50)
);

CREATE SEQUENCE dt_seq
START WITH 1
INCREMENT BY 1
NOCACHE
NOCYCLE;

CREATE TABLE Group2_Dimdate(
    Date_id NUMBER(5)  PRIMARY KEY,
    year NUMBER(4) NOT NULL,
    month  NUMBER(2) NOT NULL
);

CREATE TABLE Group2_tkt_sales(

    Theatre_id NUMBER(5) CONSTRAINT fk1 REFERENCES Group2_Dimth,
    P_id NUMBER(5) CONSTRAINT fk2 REFERENCES Group2_Dimpr,
    Client_id NUMBER(5) CONSTRAINT fk3 REFERENCES Group2_Dimcl,
    Date_id NUMBER(5) CONSTRAINT fk4 REFERENCES Group2_Dimdate,
    TotalAmount NUMBER(10,2) NOT NULL,
    CONSTRAINT Tkt_id PRIMARY KEY (Theatre_id,P_id,Client_id, Date_id)
);  

/*  INSERTING VALUES IN DATA MART USING ETL */ 

INSERT INTO Group2_Dimth
SELECT dth_seq.nextval, Theatre#, Name, Street, Town, County, MainTel 
FROM (
    SELECT DISTINCT Theatre#, lower(trim(Name)) Name, lower(trim(Street)) Street, lower(trim(Town)) Town, lower(trim(County)) County, MainTel
FROM (
    SELECT th.Theatre#, th.Name, th.Street, th.Town, th.County, th.MainTel
    FROM th, pf
    WHERE th.Theatre# = pf.Theatre#));

INSERT INTO Group2_Dimpr 
SELECT dpr_seq.nextval, P#, Title, ProductionDirector, PlayAuthor
FROM (
    SELECT DISTINCT P#, lower(trim(Title)) Title, lower(trim(ProductionDirector)) ProductionDirector, lower(trim(PlayAuthor)) PlayAuthor
FROM (
    SELECT pr.P#, pr.Title, pr.ProductionDirector, pr.PlayAuthor
    FROM pr, pf
    WHERE pr.P# = pf.P#));


INSERT INTO Group2_Dimcl
 SELECT dcl_seq.nextval, Client#, Title, Name, Street, Town, County, TelNo, Email
FROM (
    SELECT DISTINCT Client#, lower(trim(Title)) Title, upper(trim(Name)) Name, lower(trim(Street)) Street, lower(trim(Town)) Town, lower(trim(County)) County, TelNo, Email
FROM (
    SELECT cl.Client#, cl.Title, cl.Name, cl.Street, cl.Town, cl.County, cl.TelNo, cl.Email
    FROM cl, tkt_p, pf
    WHERE cl.Client# = tkt_p.Client# 
    AND tkt_p.Per# = pf.Per#));


INSERT INTO Group2_Dimdate SELECT dt_seq.nextval, year,month
FROM (SELECT DISTINCT EXTRACT(year FROM pDate) year, EXTRACT(month FROM pDate) month
FROM pf);

SELECT * FROM Group2_Dimdate;

INSERT INTO Group2_tkt_sales
SELECT *
FROM (
    SELECT 
        dth.Theatre_id,
        dpr.P_id,
        dcl.Client_id,
        ddate.Date_id,
        SUM(tkt.TotalAmount) AS TotalAmount
    FROM tkt_p tkt
    JOIN pf ON pf.Per# = tkt.Per#
    JOIN pr ON pr.P# = pf.P#
    JOIN th ON th.Theatre# = pf.Theatre#
    JOIN cl ON cl.Client# = tkt.Client#
    JOIN Group2_Dimth dth ON dth.Theatre# = th.Theatre#
    JOIN Group2_Dimpr dpr ON dpr.P# = pr.P#
    JOIN Group2_Dimcl dcl ON dcl.Client# = cl.Client#
    JOIN Group2_Dimdate ddate 
        ON ddate.year = EXTRACT(YEAR FROM pf.pDate)
       AND ddate.month = EXTRACT(MONTH FROM pf.pDate)

    GROUP BY 
        dth.Theatre_id,
        dpr.P_id,
        dcl.Client_id,
        ddate.Date_id
)
WHERE ROWNUM <= 5000;

DELETE Group2_tkt_sales;
SELECT* FROM Group2_tkt_sales 
FETCH NEXT 10 ROWS ONLY;

/* The total sale value of each production. */
/* Data Mart */
SELECT DISTINCT Group2_Dimpr.P_id, Group2_Dimpr.Title, SUM(TotalAmount) TotalAmount
FROM Group2_Dimpr, Group2_Dimdate, Group2_tkt_sales
WHERE Group2_Dimpr.P_id = Group2_tkt_sales.P_id
AND Group2_Dimdate.Date_id = Group2_tkt_sales.Date_id
GROUP BY Group2_Dimpr.P_id, Group2_Dimpr.Title;

/* OLTP */
SELECT DISTINCT pr.P#, pr.Title, SUM(TotalAmount) TotalAmount
FROM  pr, pf, tkt_p
WHERE pr.P# = pf.P#
AND pf.Per# = tkt_p.Per#
GROUP BY pr.P#, pr.Title;

/* MONTHLY SALE VALUE OF EACH THEATRE */
SELECT Group2_Dimth.Theatre_id, Group2_Dimth.Name, Group2_Dimdate.month, SUM(TotalAmount) TotalAmount
FROM Group2_Dimth, Group2_Dimdate, Group2_tkt_sales
WHERE Group2_Dimth.Theatre_id = Group2_tkt_sales.Theatre_id
AND Group2_Dimdate.Date_id = Group2_tkt_sales.Date_id
GROUP BY Group2_Dimth.Theatre_id, Group2_Dimth.Name,Group2_Dimdate.month;

/* OLTP*/
SELECT th.Theatre#, th.Name, EXTRACT(month FROM pf.pDate) month, SUM(TotalAmount) TotalAmount
FROM th, pf,tkt_p
WHERE th.Theatre#= pf.Theatre#
AND pf.Per# = tkt_p.Per#
GROUP BY th.Theatre#,th.Name,EXTRACT(month FROM pf.pDate);

/* THE THEATRE NAME (EACH) AND THE NAMES OF CLIENTS WHO HAVE THE HIGHEST SPENDING */
SELECT 
    dth.Name TheatreName,
    dcl.Name ClientName,
    SUM(ts.TotalAmount) TotalSpent
FROM Group2_tkt_sales ts, Group2_Dimth dth, Group2_Dimcl dcl
WHERE ts.Theatre_id = dth.Theatre_id
AND ts.Client_id = dcl.Client_id
GROUP BY 
    dth.Name,
    dcl.Name,
    ts.Theatre_id
HAVING SUM(ts.TotalAmount) = (
    SELECT MAX(SUM(ts2.TotalAmount))
    FROM Group2_tkt_sales ts2
    WHERE ts2.Theatre_id = ts.Theatre_id
    GROUP BY ts2.Client_id
)
ORDER BY dth.Name, TotalSpent;

/* OLTP */
SELECT 
    th.Name  TheatreName,
    cl.Name  ClientName,
    SUM(t.TotalAmount)  TotalSpent
FROM 
    tkt_p t,pf,th,cl
WHERE  t.Per# = pf.Per#
    AND pf.Theatre# = th.Theatre#
    AND t.Client# = cl.Client#
GROUP BY th.Name,cl.Name,th.Theatre#
HAVING SUM(t.TotalAmount) = (
        SELECT MAX(SUM(t2.TotalAmount))
        FROM tkt_p t2, pf pf2
        WHERE t2.Per# = pf2.Per#
          AND pf2.Theatre# = th.Theatre#
        GROUP BY t2.Client#
    )
ORDER BY th.Name,TotalSpent;
