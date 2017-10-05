use Pokemon;
select * from Trainer where hometown = 'Blue City';
select * from Trainer where hometown = 'Blue City' or hometown = 'Rainbow City';
select name, hometown from Trainer where name like 'a%' or name like 'e%' or name like 'i%' or name like 'o%' or name like 'u%';
select name from Pokemon where type = 'Water';
select distinct type from Pokemon;
select name from Pokemon order by name;
select name from Pokemon where name like '%s';
select * from Pokemon where name like '%e%s';
select name from Pokemon where name like 'a%' or name like 'e%' or name like 'i%' or name like 'o%' or name like 'u%';
select type, count(type) from Pokemon group by type;
select nickname from CatchedPokemon order by level desc limit 3;
select sum(level)/count(level)as AVERAGE_LEVEL from CatchedPokemon;
select max(level)-min(level) from CatchedPokemon;
select count(name) from Pokemon where name like 'b%' or name like 'c%' or name like 'd%' or name like 'e%';
select count(name) from Pokemon where type not in ('Fire','Grass','Water','Electric');
select Trainer.name as TrainerName, Pokemon.name as PokemonName, CatchedPokemon.nickname from CatchedPokemon, Pokemon, Trainer where nickname like '% %' and Pokemon.id = CatchedPokemon.pid and CatchedPokemon.owner_id = Trainer.id;
select Trainer.name from CatchedPokemon, Trainer, Pokemon where CatchedPokemon.owner_id = Trainer.id and Pokemon.type = 'psychic' and CatchedPokemon.pid = Pokemon.id;
select Trainer.name, Trainer.hometown from CatchedPokemon, Trainer where owner_id = Trainer.id group by name order by avg(level) desc limit 3;
select Trainer.name, count(Trainer.name) 
from Trainer, CatchedPokemon 
where Trainer.id = CatchedPokemon.owner_id group by Trainer.id order by count(Trainer.name) desc;
select Pokemon.name, CatchedPokemon.level 
from Gym, CatchedPokemon, Pokemon
where Gym.city = 'Sangnok City' and Gym.leader_id = CatchedPokemon.owner_id and Pokemon.id = CatchedPokemon.pid order by level;
select P.name, count(CatchedList.name) from Pokemon P left join ( select Pokemon.name from Pokemon, CatchedPokemon where Pokemon.id = CatchedPokemon.pid ) CatchedList on P.name = CatchedList.name group by P.name order by count(CatchedList.name) desc;
select P.name from Pokemon P,Evolution E
where P.id = E.after_id and E.before_id in
( select Evolution.after_id from Pokemon,Evolution 
where Pokemon.name = 'charmander' and Evolution.before_id = Pokemon.id );
select Pokemon.name from Pokemon where Pokemon.id<=30 and Pokemon.id in( select pid from CatchedPokemon);
select Trainer.name, Pokemon.type from Trainer, Pokemon, CatchedPokemon where CatchedPokemon.owner_id = Trainer.id and Pokemon.id = CatchedPokemon.pid group by name having count(distinct type) =1;
select Trainer.name, Pokemon.type, count(Trainer.name) from Trainer, CatchedPokemon, Pokemon where Trainer.id = CatchedPokemon.owner_id and Pokemon.id = CatchedPokemon.pid group by Trainer.name, type;
select Trainer.name, Pokemon.name, count(Trainer.name) from Trainer, CatchedPokemon, Pokemon where Trainer.id = CatchedPokemon.owner_id and Pokemon.id = CatchedPokemon.pid group by Trainer.name having count(distinct Pokemon.id)=1;
select Trainer.name, Trainer.hometown from Gym, Trainer, CatchedPokemon, Pokemon where Gym.leader_id = Trainer.id and Trainer.id = CatchedPokemon.owner_id and Pokemon.id = CatchedPokemon.pid group by Trainer.name having count(distinct type)!=1;
select Trainer.name, C.SUM 
from Trainer, Gym G left join (
	select Gym.city, sum(level) as SUM
	from Gym, Trainer, CatchedPokemon, Pokemon
	where Gym.leader_id = Trainer.id
	and CatchedPokemon.owner_id = Trainer.id
	and level>=50
	and Pokemon.id = CatchedPokemon.pid
	group by Trainer.name ) as C on G.city = C.city
where Trainer.id = G.leader_id;
select Sangnok.name from 
( select Pokemon.name from CatchedPokemon, Trainer, Pokemon
where CatchedPokemon.owner_id = Trainer.id 
and Pokemon.id = CatchedPokemon.pid
and Trainer.hometown = 'Sangnok City' ) as Sangnok join
( select Pokemon.name from CatchedPokemon, Trainer, Pokemon
where CatchedPokemon.owner_id = Trainer.id 
and Pokemon.id = CatchedPokemon.pid
and Trainer.hometown = 'Blue City' ) as Blue on Sangnok.name = Blue.name;
select Pokemon.name from Pokemon where Pokemon.id in
( select after_id from Pokemon, Evolution 
where Pokemon.id = Evolution.after_id )
and Pokemon.id not in
( select before_id from Pokemon, Evolution 
where Pokemon.id = before_id );
