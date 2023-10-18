--Populate Animal Dimension Table
insert
	into
	dim_animal ("Animal ID",
	"Name",
	"Date of Birth",
	"Animal Type",
	"Breed",
	"Color")
select
	distinct on
	("Animal ID") "Animal ID",
	"Name",
	"Date of Birth",
	"Animal Type",
	"Breed",
	"Color"
from
	outcomes;

--Populate Outcome Dimension Table
insert
	into
	dim_outcome_type ("Outcome Type",
	"Outcome Subtype")
select
	distinct on
	("Outcome Type",
	"Outcome Subtype") "Outcome Type",
	"Outcome Subtype"
from
	outcomes;

--Populate Outcome Date Dimension Table
insert
	into
	dim_outcome_date ("Date of Outcome",
	"Outcome Day",
	"Outcome Month",
	"Outcome Year",
	"Outcome Day of Week")
select 
	distinct on
	("Date of Outcome") "Date of Outcome",
	"Outcome Day",
	"Outcome Month",
	"Outcome Year",
	"Outcome Day of Week"
from
	(
	select
		"Date of Outcome"
    ,
		extract('DAY'
	from
		"Date of Outcome") as "Outcome Day"
	,
		to_char("Date of Outcome",
		'Month') as "Outcome Month"
	,
		extract('YEAR'
	from
		"Date of Outcome") as "Outcome Year"
	,
		to_char("Date of Outcome"
		,
		'Day'
		) as "Outcome Day of Week"
	from
		outcomes
)
order by
	"Date of Outcome";

--Populate Reproductive Status Dimension Table
insert
	into
	dim_rp_status ("Reproductive Status upon Outcome",
	"Sex upon Outcome")
select
	distinct on
	("Reproductive Status upon Outcome",
	"Sex upon Outcome") "Reproductive Status upon Outcome",
	"Sex upon Outcome"
from
	outcomes;

--Populate Age at Outcome Dimension Table
insert
	into
	dim_age_at_outcome ("Animal Type",
	"Age Years"
	)
select
	distinct on
	("Animal Type",
	"Years Age upon Outcome") "Animal Type",
	"Years Age upon Outcome"
from
	outcomes;

update
	dim_age_at_outcome
set
	"Animal Age Category" = 'Kitten'
where
	"Animal Type" = 'Cat'
	and "Age Years" < 2;

update
	dim_age_at_outcome
set
	"Animal Age Category" = 'Adult'
where
	"Animal Type" = 'Cat'
	and "Age Years" < 11
    and "Age Years" > 1;

update
	dim_age_at_outcome
set
	"Animal Age Category" = 'Senior'
where
	"Animal Type" = 'Cat'
	and "Age Years" > 10;

--Populate Outcome Fact Table
insert
	into
	fct_outcome ("Animal ID",
	"Outcome Date ID",
	"Outcome Type ID",
    "Age at Outcome ID",
	"Reproductive Status ID")
select
	o."Animal ID",
	dod."Outcome Date ID",
	dot."Outcome Type ID",
    daao."Age ID",
	drs."RP Status ID"
from
	outcomes o,
	dim_outcome_date dod,
	dim_outcome_type dot,
    dim_age_at_outcome daao,
	dim_rp_status drs
where
	o."Date of Outcome" = dod."Date of Outcome"
	and o."Outcome Type" = dot."Outcome Type"
	and o."Outcome Subtype" = dot."Outcome Subtype"
    and o."Years Age upon Outcome" = daao."Age Years"
    and o."Animal Type" = daao."Animal Type"
	and drs."Reproductive Status upon Outcome" = o."Reproductive Status upon Outcome"
	and drs."Sex upon Outcome" = o."Sex upon Outcome";

--Commit changes to data warehouse else above commands will not reflect in DBeaver    
COMMIT;