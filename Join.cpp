#include "Join.hpp"

#include <vector>

using namespace std;

/*
 * Input: Disk, Memory, Disk page ids for left relation, Disk page ids for right relation
 * Output: Vector of Buckets of size (MEM_SIZE_IN_PAGE - 1) after partition
 */

/*
GRACE HASH ALGORITHM

PARTITION
for each tuple in relation r in R: 
	put r in bucket (output buffer) h1(r)
flush B-1 buffers to disk

for each tuple in relation s in S: 
	put s in bucket (output buffer) h1(s)
flush B-1 buffers to disk

PROBE
for k = 1 to B - 1 do
	for each tuple r in R_k do
		put r in bucket h2(r)
	for each tuple s in S_k do
		for each tuple r in bucket h2(s) do 
			if r = s then
				put (r,s) in the output relation
*/

vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel, pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk)); // output buffers

	int mem_page = 0;
	int left_rel_index = left_rel.first; 

	// load in pages into memory 
	while (left_rel_index < left_rel.second && mem_page < MEM_SIZE_IN_PAGE) {
		mem->loadFromDisk(disk, left_rel_index, mem_page);
		left_rel_index++;
		mem_page++;
	}

	// go through pages loaded into memory
	for (uint p = 0; p < MEM_SIZE_IN_PAGE; ++p) {
		Page *curr_page = mem->mem_page(p);
		for (uint r = 0; r < curr_page->size(); ++r) {
			Record curr_record = curr_page->get_record(r);
			uint hash_value = curr_record.partition_hash(); // hash on tuple in r
			// load record into a page? 
			partitions[hash_value].add_left_rel_page(p);
		}	
	}

	for (int partition = 0; partition < partitions.size(); ++partition) {
		vector<uint> left_rel_pages = partitions[partition].get_left_rel();
		for (uint page_id : left_rel_pages) {
			
		}
	}
	

	        // partition s using hash function
	        // bring a page of s to memory at a time
	        // store each tuple hash(s) into output bucket
	        // bucket fills --> write to disk
	        return partitions;
}

/*
 * Input: Disk, Memory, Vector of Buckets after partition
 * Output: Vector of disk page ids for join result
 */
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; // placeholder
	return disk_pages;
}
