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

 /*
 PROBE
for k = 1 to B - 1 do
	for each tuple r in R_k do
		put r in bucket h2(r)
	for each tuple s in S_k do
		for each tuple r in bucket h2(s) do 
			if r = s then
				put (r,s) in the output relation
				*/
vector<uint> probe(Disk* disk, Mem* mem, vector<Bucket>& partitions) {
	// TODO: implement probe phase
	vector<uint> disk_pages; // placeholder
	std::unordered_map<int, vector<Record>> r_hash;
	// go through each page in r
	uint out_buffer_page = 1;
	uint input_buffer = 0;
	for (uint partition = 0; partition < partitions.size(); partition++) {
		for (uint page_no = 0; page_no < partitions[partition].num_left_rel_record; page_no++) {
			Page *curr_page = mem->mem_page(partitions[partition].get_left_rel()[page_no]);
			
			// for every tuple r in R_k
			for (uint r_tuple = 0; r_tuple < RECORDS_PER_PAGE; r_tuple++) {
				Record curr_tuple = curr_page->get_record(r_tuple);
				uint hash_idx = curr_tuple.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
				if (r_hash.find(hash_idx) != r_hash.end()) { // checks if hash_idx exists 
					r_hash[hash_idx].push_back(curr_tuple);
				} else {
					r_hash[hash_idx] = 
				}
			}
			for (uint s_page = 0; s_page < partitions[partition].num_right_rel_record; s_page++) {
				mem->loadFromDisk(disk, s_page, input_buffer);
				// for every tuple s in S_k
				for(uint s_tuple = 0; s_tuple < mem->mem_page(input_buffer)->size(); s_tuple++) {
					Record curr_s_tuple = mem->mem_page(partitions[partition].get_right_rel()[page_no])->get_record(s_tuple);
					uint s_hash_idx = curr_s_tuple.probe_hash() % (MEM_SIZE_IN_PAGE - 2);
					
					vector<Record> same_hash = r_hash[s_hash_idx];
					
					for(uint r_tuple = 0; r_tuple < same_hash.size(); r_tuple++) {
						if (mem->mem_page(out_buffer_page)->full()) { // flush to disk
							int disk_page = mem->flushToDisk(disk, out_buffer_page);
							disk_pages.push_back(disk_page);
							out_buffer_page += 1;
						}
						if (same_hash[r_tuple] == curr_s_tuple) {
							mem->mem_page(out_buffer_page)->loadPair(same_hash[r_tuple], curr_s_tuple);
						}
					}
					
				}
			}
		}
	}
	// flush output buffers to disk --> occurs if there are any remaining tuples 
	if (!(mem->mem_page(out_buffer_page)->empty())) { // if the buffer is not empty
		int disk_page = mem->flushToDisk(disk, out_buffer_page);
		disk_pages.push_back(disk_page);
	}


	return disk_pages;
}

