/*
drop table if exists linktest;
*/
/*
create table linktest (
    src TEXT,
    dst TEXT
);
*/
/*
insert into linktest values ("B", "A"), ("C", "B"), ("D", "A"), ("E", "D"), ("F", "E"), ("G", "D");
*/

--insert into linktest values ("XA1", "XROOT"), ("XA2", "XROOT"), ("XB1", "XA1");

with descendants as ( 
                        -- Leaves are nodes that are only destinations, never sources 
                        -- select * from linktest where src not in (select distinct dst from linktest)
                        -- Roots are destinations that don't lead anywhere (they don't appear as sources)
                        select * from linktest where dst not in (select distinct src from linktest)
                      
                      UNION ALL
                      
                      -- distinct in hopes of preventing loops destroying the recursion
                      select 
                          distinct l.src, d.dst 
                      from descendants d
                      inner join linktest l on l.dst = d.src
                      
                  )
                  
select * from descendants