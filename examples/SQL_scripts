select tupleid, rid, count(*) as tupledegree from violation 
group by rid, tupleid
order by tupledegree

CREATE VIEW tuple_degree as select tupleid, rid, count(*) as degree from violation 
group by rid, tupleid
order by degree desc;

CREATE VIEW cell_degree as select tupleid, attribute, count(*) as degree from violation
group by tupleid, attribute
order by degree desc;

select vid, V.rid, V.tupleid, V.attribute, F1.degree as tuple_degree, F2.degree as cell_degree from violation as V, tuple_degree as F1, cell_degree as F2
where V.tupleid = F1.tupleid and V.rid=F1.rid and V.tupleid=F2.tupleid and V.attribute=F2.attribute
order by tuple_degree, cell_degree; 
