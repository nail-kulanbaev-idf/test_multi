select
    toDate(uploadDate) as dt,
    count(*) as cnt
from mx_master.cdc_parsed_response
group by dt
order by dt;

insert into mx_master.cdc_parsed_response
select *
from remote('172.31.101.101', 'mx_master', 'cdc_parsed_response', 'nail_kulanbaev', 'Eiquexee5roopietheiv')
where uploadDate >= today() - 2
  and uploadDate < today();

select
    toDate(uploadDate) as dt,
    count(*) as cnt
from remote('172.31.101.101', 'mx_master', 'cdc_parsed_response', 'nail_kulanbaev', 'Eiquexee5roopietheiv')
where uploadDate >= today() - 2
  and uploadDate < today()
group by dt
order by dt;

show create table mx_scoring.score_cdc_nn;

select *
from mx_debezium.credit;
