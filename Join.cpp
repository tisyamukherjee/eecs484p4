#include "Join.hpp"

#include <vector>
#include <iostream>
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

// left_rel == RELATION R left_rel.first is starting page of the memory, .second is ending 
// right_rel == RELATION S
vector<Bucket> partition(Disk* disk, Mem* mem, pair<uint, uint> left_rel, pair<uint, uint> right_rel) {
	// TODO: implement partition phase
	vector<Bucket> partitions(MEM_SIZE_IN_PAGE - 1, Bucket(disk)); // partitions for PROBE

	// assuming disk is loaded with relation
	// loop through the disk page, take a page into input buffer, hash the tuples 

	// RELATION R
	int input_buffer = 0; 
	int output_buffer_id = 0;
	for (uint page = left_rel.first; page < left_rel.second; page++) { // for each page in the disk holding R
		// load a page of R into input buffer
		mem->loadFromDisk(disk, page, input_buffer);
		// iterate through records (tuples) in the page, apply hash, then into output buffer
		for (uint tuple = 0; tuple < mem->mem_page(input_buffer)->size(); tuple++) {
			// within the memory page of the input buffer, get the tuple in the input buffer
			Record curr_tuple = mem->mem_page(input_buffer)->get_record(tuple);

			// get the hash value of the output buffer, mod mem_size_in_page - 1 bc spec said
			output_buffer_id = curr_tuple.partition_hash() % (MEM_SIZE_IN_PAGE - 1); 
			
			uint out_buffer_page = output_buffer_id + 1; // memory where output buffer is located 
			// + 1 bc 0 is used from input_buffer
	
			if (mem->mem_page(out_buffer_page)->full()) { // flush to disk
				uint disk_page_id = mem->flushToDisk(disk, out_buffer_page);
				partitions[output_buffer_id].add_left_rel_page(disk_page_id);
			}
			// add tuple into the buffer
			mem->mem_page(out_buffer_page)->loadRecord(curr_tuple);

		}
	}

	// flush output buffers to disk --> occurs if there are any remaining tuples 
	for (uint out_buff = 0; out_buff < MEM_SIZE_IN_PAGE - 1; out_buff++) {
		if (!(mem->mem_page(out_buff + 1)->empty())) { // if the buffer is not empty
			uint disk_page_id = mem->flushToDisk(disk, out_buff + 1);
			partitions[out_buff].add_left_rel_page(disk_page_id);
		}
	}


	// RIGHT RELATION --> RELATION S
	input_buffer = 0; 
	output_buffer_id = 0;
	for (uint page = right_rel.first; page < right_rel.second; page++) { // for each page in the disk holding S
		// load a page of S into input buffer
		mem->loadFromDisk(disk, page, input_buffer);
		// iterate through records (tuples) in the page, apply hash, then into output buffer
		for (uint tuple = 0; tuple < mem->mem_page(input_buffer)->size(); tuple++) {
			// within the memory page of the input buffer, get the tuple in the input buffer
			Record curr_tuple = mem->mem_page(input_buffer)->get_record(tuple);

			// get the id of the output buffer, mod mem_size_in_page bc spec said
			output_buffer_id = curr_tuple.partition_hash() % (MEM_SIZE_IN_PAGE - 1); 
			
			uint out_buffer_page = output_buffer_id + 1; // memory where output buffer is located 
			/* Return true if this page is full of data records */
	
			if (mem->mem_page(out_buffer_page)->full()) { // flush to disk
				uint disk_page_id = mem->flushToDisk(disk, out_buffer_page);
				partitions[output_buffer_id].add_right_rel_page(disk_page_id);
			}
			// add tuple into the buffer
			mem->mem_page(out_buffer_page)->loadRecord(curr_tuple);

		}
	}

	// flush output buffers to disk --> occurs if there are any remaining tuples 
	for (uint out_buff = 0; out_buff < MEM_SIZE_IN_PAGE - 1; out_buff++) {
		if (!(mem->mem_page(out_buff + 1)->empty())) { // if the buffer is not empty
			uint disk_page_id = mem->flushToDisk(disk, out_buff + 1);
			partitions[out_buff].add_right_rel_page(disk_page_id);
		}
	}
	// int mem_page = 0;
	// int left_rel_index = left_rel.first; 


	// // load in pages into memory 
	// while (left_rel_index < left_rel.second && mem_page < MEM_SIZE_IN_PAGE) {
	// 	mem->loadFromDisk(disk, left_rel_index, mem_page);
	// 	left_rel_index++;
	// 	mem_page++;
	// }
	// // go through pages loaded into memory
	// for (uint p = 0; p < MEM_SIZE_IN_PAGE; ++p) {
	// 	Page *curr_page = mem->mem_page(p);
	// 	for (uint r = 0; r < curr_page->size(); ++r) {
	// 		Record curr_record = curr_page->get_record(r);
	// 		uint hash_value = curr_record.partition_hash(); // hash on tuple in r
	// 		// load record into a page? 
	// 		partitions[hash_value].add_left_rel_page(p);
	// 	}	
	// }

	// for (int partition = 0; partition < partitions.size(); ++partition) {
	// 	vector<uint> left_rel_pages = partitions[partition].get_left_rel();
	// 	for (uint page_id : left_rel_pages) {
			
	// 	}
	// }
	

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
