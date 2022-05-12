Noah Yuen - CSC 369 - Lab 4

1. Request count for each country

For the first query, I had three map-reduce phases.

For the first map-reduce phase, the access log map function reads each line of the input access log file and outputs
the hostname as the key and a map containing a source identifier. The host country mapper reads each line of the input
csv file and outputs the hostname as the key and a map containing a source identifier and the name of the country.
The join reducer of the first phase iterates through each of the 'values', which are maps, and checks
if the source is the access log or host country csv. If the source is the access log, the running sum is
incremented. If the source is the host country csv, the country name is stored in a variable. After all
values have been checked, the join reducer emits the count as the key and country name as the value.

In the second map-reduce phase, the map function simply emits the country name as the key and the count
value as the value. The reducer then sums up all the counts for each country and emits the sum as the key
and country name as the value.

In the third and final map-reduce phase, the map function simply emits the count as the key and country
name as the value, and the comparator sorts by decreasing request count.

2. Url count for each country

For the second query, I had two map-reduce phases.

For the first map-reduce phase, the access log mapper read each line of the input access log file and emitted
the host name as the key and a map containing a source identifier and the url of the current log. The host country
mapper read each line of the input csv file and emitted the hostname as the key and a map containing a source
identifier and the country name. In the join reducer, I iterated through each of the values and checked the source
identifier. If the value was from the access log, I added the url to a map containing urls mapped to their running
counts, or I incremented the count of the current url if it was already in the map. If the value was from the host
country csv file, I stored the country name in a variable. After all values had been checked, I iterated through
each of the urls in the map, and emitted the country name as the key, and the url/count pair as the value.

For the second and final map-reduce phase, I created a composite key containing the country name and count of each
country/url pair. I also created a comparator to sort by country name (alphabetical order) and descending count value.
The output of the final map-reduce phase was the country name as the key and the count and url as the value.
