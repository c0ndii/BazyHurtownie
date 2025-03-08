create table Zip (
    id_zip number(5) not null PRIMARY KEY,
    zip varchar2(5) unique,
    state varchar2(2)
);

create table Time (
    id_time number(3) not null primary key,
    month number(2),
    year number(4),
    constraint unique_month_year UNIQUE(month, year)
);

create table Sale (
    id_sale number(8) primary key,
    id_zip number(5) references Zip(id_zip),
    id_time number(3) references Time(id_time),
    amount number(10,2),
    constraint unique_zip_time UNIQUE(id_zip, id_time)
);

create table Profit_by_year (
    year number(4) primary key,
    profit number(10,2)
);

create sequence S_zip increment by 1 minvalue 1 nomaxvalue nocycle;

create sequence S_sale increment by 1 minvalue 1 nomaxvalue nocycle;

create sequence S_time;

create or replace trigger Trig_set_id_zip
before insert on Zip
for each row
begin
    :NEW.id_zip := S_zip.nextval;
end;
/
insert into Zip (zip, state) values ('15217','PA');

create or replace trigger Trig_set_id_time
before insert on Time
for each row
begin
    :NEW.id_time := S_time.nextval;
end;
/

create or replace trigger Trig_set_id_sale
before insert on Sale
for each row
begin
    :NEW.id_sale := S_sale.nextval;
end;
/

create or replace function F_profit_by_year(
    P_year in number) return number is
    V_profit number;
BEGIN
    select SUM(OI.QUANTITY*(B.RETAIL-B.COST))
    INTO V_profit
    from BOOKS B join ORDERITEMS OI on B.ISBN=OI.ISBN
        join ORDERS O ON OI.ORDER#=o.ORDER#
    where EXTRACT(year from O.ORDERDATE) = P_year;
    return V_profit;
END;
/

select F_profit_by_year(2010) from dual;

select distinct extract(year from orderdate), F_profit_by_year(extract(year from orderdate)) from ORDERS;

create or replace procedure P_load_data_into_profit_by_year is
cursor C_profit_by_year is 
    select distinct extract(year from orderdate) YEAR, 
    F_profit_by_year(extract(year from orderdate)) PROFIT
    from ORDERS order by 1;
    V_year number(1);
begin
    for I in C_profit_by_year loop
        select count(1) INTO V_year
        from Profit_by_year WHERE Year = I.Year;
        if V_year = 0 then
            insert into profit_by_year values(I.year, I.profit);
        else
            update profit_by_year set profit=I.profit where year =I.year;
        end if;
    end loop;
end;
/

execute P_load_data_into_profit_by_year;

select * from profit_by_year;

create or replace view V_SALE_AMOUNT_BY_TIME_ZIP AS 
    select 
        Z.ID_ZIP, T.ID_TIME,
        sum(OI.QUANTITY*B.RETAIL) AMOUNT
    from BOOKS B join ORDERITEMS OI on B.ISBN=OI.ISBN
        join ORDERS O ON OI.ORDER#=o.ORDER# join 
        CUSTOMERS C on C.CUSTOMER# = O.CUSTOMER# JOIN
        ZIP Z ON Z.ZIP=C.ZIP JOIN
        TIME T ON T.MONTH = EXTRACT(MONTH FROM ORDERDATE) AND
            T.YEAR = EXTRACT(YEAR FROM ORDERDATE)
    GROUP BY 
        Z.ID_ZIP, T.ID_TIME;
    
create or replace procedure p_data_migration is
begin
    -- load into zip 
    merge into Zip Z 
    using (select distinct ZIP, State from Customers) sub
    on (Z.ZIP=sub.ZIP)
    when not matched then
        insert(ZIP, State) values(sub.ZIP, sub.state);
    
    -- load into time
    merge into Time T
    using (select distinct extract(month from ORDERDATE) month, extract(year from ORDERDATE) year from Orders) sub
    on(T.month=sub.month and T.year = sub.year)
    when not matched then
        insert(month, year) values(sub.month, sub.year);
        
    -- load into sale
    merge into Sale S
    using V_SALE_AMOUNT_BY_TIME_ZIP V
    on(V.ID_ZIP=S.ID_ZIP and V.ID_TIME=S.ID_TIME)
    when not matched then
        insert(ID_ZIP, ID_TIME, AMOUNT) values(V.ID_ZIP, V.ID_TIME, V.AMOUNT)
    when matched then
        UPDATE SET AMOUNT = V.AMOUNT;
    
end;
/

CREATE OR REPLACE PACKAGE PACKAGE_DATA_MIGRATION AS
    PROCEDURE P_DATA_MIGRATION;
    FUNCTION F_PROFIT_BY_YEAR(P_YEAR IN NUMBER) RETURN NUMBER;
    PROCEDURE P_LOAD_DATA_INTO_PROFIT_BY_YEAR;
END;
/

CREATE OR REPLACE PACKAGE BODY PACKAGE_DATA_MIGRATION AS
--
procedure p_data_migration is
begin
    -- load into zip 
    merge into Zip Z 
    using (select distinct ZIP, State from Customers) sub
    on (Z.ZIP=sub.ZIP)
    when not matched then
        insert(ZIP, State) values(sub.ZIP, sub.state);
    
    -- load into time
    merge into Time T
    using (select distinct extract(month from ORDERDATE) month, extract(year from ORDERDATE) year from Orders) sub
    on(T.month=sub.month and T.year = sub.year)
    when not matched then
        insert(month, year) values(sub.month, sub.year);
        
    -- load into sale
    merge into Sale S
    using V_SALE_AMOUNT_BY_TIME_ZIP V
    on(V.ID_ZIP=S.ID_ZIP and V.ID_TIME=S.ID_TIME)
    when not matched then
        insert(ID_ZIP, ID_TIME, AMOUNT) values(V.ID_ZIP, V.ID_TIME, V.AMOUNT)
    when matched then
        UPDATE SET AMOUNT = V.AMOUNT;
    
END P_DATA_MIGRATION;

--

    function F_profit_by_year(
    P_year in number) return number is
    V_profit number;
BEGIN
    select SUM(OI.QUANTITY*(B.RETAIL-B.COST))
    INTO V_profit
    from BOOKS B join ORDERITEMS OI on B.ISBN=OI.ISBN
        join ORDERS O ON OI.ORDER#=o.ORDER#
    where EXTRACT(year from O.ORDERDATE) = P_year;
    return V_profit;
END F_PROFIT_BY_YEAR
;
    procedure P_load_data_into_profit_by_year is
cursor C_profit_by_year is 
    select distinct extract(year from orderdate) YEAR, 
    F_profit_by_year(extract(year from orderdate)) PROFIT
    from ORDERS order by 1;
    V_year number(1);
begin
    for I in C_profit_by_year loop
        select count(1) INTO V_year
        from Profit_by_year WHERE Year = I.Year;
        if V_year = 0 then
            insert into profit_by_year values(I.year, I.profit);
        else
            update profit_by_year set profit=I.profit where year =I.year;
        end if;
    end loop;
end P_load_data_into_profit_by_year;
END PACKAGE_DATA_MIGRATION;
/

drop function F_PROFIT_BY_YEAR;
drop procedure P_load_data_into_profit_by_year;
drop procedure P_DATA_MIGRATION;

execute PACKAGE_DATA_MIGRATION.P_DATA_MIGRATION;

select PACKAGE_DATA_MIGRATION.F_PROFIT_BY_YEAR(2010) FROM DUAL;

create or replace trigger Trig_control_retail_cost
before insert or update of Retail, Cost on Books
for each row
begin
    if inserting then
        if :New.Retail > :New.Cost*2 OR :New.Retail < :New.Cost then
            :New.Retail := :New.Cost*2;
        end if;
    else --updating
        if :New.Retail is null then
            :New.Retail := :New.Cost*2;
        end if;
    end if; 
end;
/

