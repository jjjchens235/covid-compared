
db_fiddle_url = 'https://www.db-fiddle.com/f/hRZGF5beVzoKPvNGm8wcGr/2'

#confirmed
create table us_confirmed (dt date, country varchar(10), state varchar(20), county varchar(100), confirmed int);

insert into us_confirmed (dt, country, state, county, confirmed)
values ('2020-05-01', 'USA', 'CA', 'Los Angeles', 5);

insert into us_confirmed (dt, country, state, county, confirmed)
values ('2020-05-02', 'USA', 'CA', 'Riverside', 9);


#deaths
create table us_deaths (dt date, country varchar(10), state varchar(20), county varchar(100), deaths int);

insert into us_deaths (dt, country, state, county, deaths)
values ('2020-05-01', 'USA', 'CA', 'Los Angeles', 8);

insert into us_deaths (dt, country, state, county, deaths)
values ('2020-05-02', 'USA', 'AK', 'Juneau', 1);



create table us_recovered (dt date, country varchar(10), state varchar(20), county varchar(100), recovered int);

insert into us_recovered (dt, country, state, county, recovered)
values ('2020-05-09', 'USA', 'CA', 'Los Angeles', 1);



select coalesce(d.dt, c.dt, r.dt) dt, coalesce(d.country, c.country, r.country) country, coalesce(d.state, c.state, r.state) state,  coalesce(d.county, c.county, r.county) county,  coalesce(d.deaths, 0) deaths, coalesce(c.confirmed, 0) confirmed, coalesce(r.recovered, 0) recovered from us_deaths d
full outer join us_confirmed c on d.county = c.county and d.state = c.state and d.dt = c.dt
full outer join us_recovered r on r.county = d.county and r.state = d.state and r.dt = d.dt
