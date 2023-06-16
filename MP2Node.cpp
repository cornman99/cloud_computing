/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode

	curMemList.emplace_back(Node(memberNode->addr));
	
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	bool need_stable;
	int self=0;
	
	for(int i = 0; i < ring.size(); i++){
		if((memberNode->addr == ring[i].nodeAddress)){
			self = i;
			break;
		}
	}

	for(int i = 0; i < ring.size(); i++){
		int j=0;
		for(;j<curMemList.size();j++){
			if(ring[i].nodeAddress==curMemList[j].nodeAddress)
				break;	
		}
		if(j==curMemList.size() && ((self-i+ring.size())%ring.size())<= 2 )
			need_stable=1;
	}//prev 2 && next 2
	ring = curMemList;
	if(need_stable){
		stabilizationProtocol();
	}
	

}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}
// transID::fromAddr::CREATE::key::value::ReplicaType
/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);
	 request* req = new request(g_transID,  this->par->getcurrtime(), CREATE, key, value);
	 undone[g_transID]=req;
	 
	 for(Node& n:pos){
		 
		 Message msg (g_transID,this->memberNode->addr,CREATE,key,value,PRIMARY);
		 emulNet->ENsend(&memberNode->addr, &n.nodeAddress,  msg.toString());
	 }
	 ++g_transID;
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
 // transID::fromAddr::READ::key
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);
	 request* req = new request(g_transID,  this->par->getcurrtime(), READ, key, "");
	 undone[g_transID]=req;
	 for(Node& n:pos){
		 
		 Message msg (g_transID,this->memberNode->addr,READ,key);
		 emulNet->ENsend(&memberNode->addr, &n.nodeAddress,  msg.toString());
	 }
	 ++g_transID;
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);
	 request* req = new request(g_transID,  this->par->getcurrtime(), UPDATE, key, value);
	 undone[g_transID]=req;
	 for(Node& n:pos){
		 
		 Message msg (g_transID,this->memberNode->addr,UPDATE,key,value,PRIMARY);
		 emulNet->ENsend(&memberNode->addr, &n.nodeAddress,  msg.toString());
	 }
	 ++g_transID;
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);
	 request* req = new request(g_transID,  this->par->getcurrtime(), DELETE, key,"");
	 undone[g_transID]=req;
	 for(Node& n:pos){
		 
		 Message msg (g_transID,this->memberNode->addr,DELETE,key);
		 emulNet->ENsend(&memberNode->addr, &n.nodeAddress,  msg.toString());
	 }
	 ++g_transID;
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	if( this->ht->create(key, value))
		return true;

	return false;
	
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	string ret = this->ht->read(key);

	return ret;
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	if( this->ht->update(key, value))
		return true;
	
	return false;
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table

	
	if (this->ht->deleteKey(key)) 
		return true;
		
	return false;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);
		
		/*
		 * Handle the message types here
		 */
		Message msg(message);
		switch(msg.type){
			case MessageType::CREATE:{
				bool succ = createKeyValue(msg.key, msg.value, msg.replica);
				if(msg.transID<0)break;
				reply(msg.transID, &msg.fromAddr, msg.type, succ, "");
				if(succ)
					log->logCreateSuccess(&memberNode->addr, 0, msg.transID, msg.key, msg.value);
				else log->logCreateFail(&memberNode->addr, 0,  msg.transID, msg.key, msg.value);
				break;
			}
			case MessageType::READ:{
				string content = readKey(msg.key);
				bool succ = content.size();
				reply(msg.transID, &msg.fromAddr, msg.type, succ, content);
				if (succ) log->logReadSuccess(&memberNode->addr, 0, msg.transID, msg.key, content);
				else log->logReadFail(&memberNode->addr, 0, msg.transID, msg.key);
				break;
			}
			case MessageType::UPDATE:{
				bool succ = updateKeyValue(msg.key, msg.value, msg.replica);
				reply(msg.transID, &msg.fromAddr, msg.type, succ, "");
				if( succ)log->logUpdateSuccess(&memberNode->addr, 0, msg.transID, msg.key,  msg.value);
				else 
					log->logUpdateFail(&memberNode->addr, 0, msg.transID, msg.key,  msg.value);	
				break;
			}
			case MessageType::DELETE:{
				bool succ = deletekey(msg.key);
				reply(msg.transID, &msg.fromAddr, msg.type, succ, "");
				if (succ) log->logDeleteSuccess(&memberNode->addr, 0, msg.transID, msg.key);
				else log->logDeleteFail(&memberNode->addr, 0,msg.transID, msg.key);
				break;
			}
			case MessageType::READREPLY:{
				if(!undone.count(msg.transID))break;
				if(msg.value.size()) 
					undone[msg.transID]->quorum++;
				undone[msg.transID]->replies ++;
				undone[msg.transID]->value = msg.value;
				break;
			}
			case MessageType::REPLY:{
				if(!undone.count(msg.transID))break;
				if(msg.success)
					undone[msg.transID]->quorum++;
				undone[msg.transID]->replies ++;

				break;
			}
		}
		 

	}
	check_request();
	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}
/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	 for(auto [k,v]:this->ht->hashTable){
	 	stableCreate(k,v);
	 }
	 return;
}
void MP2Node::stableCreate(string key, string value) {
	/*
	 * Implement this
	 */
	 vector<Node>pos=findNodes(key);

	 
	 for(Node& n:pos){
		 Message msg (-777,this->memberNode->addr,CREATE,key,value,PRIMARY);
		 emulNet->ENsend(&memberNode->addr, &n.nodeAddress,  msg.toString());
	 }
}


request::request(int _id, int _timestamp, MessageType _msg_Type, string _key, string _value){

	this->id = _id;
	this->timestamp = _timestamp;
	this->replies = 0;
	this->quorum = 0;
	this->msg_Type = _msg_Type;
	this->key = _key;
	this->value = _value;
}

void MP2Node::reply(int transID, Address* fromAddr, MessageType type, bool success, string value){
	if(type != MessageType::READ){
		Message msg(transID, this->memberNode->addr,  MessageType::REPLY, success);
		emulNet->ENsend(&memberNode->addr, fromAddr, msg.toString());
		
	}else{
		Message msg(transID, this->memberNode->addr, value);
		emulNet->ENsend(&memberNode->addr, fromAddr, msg.toString());	
		
	}
	
}

void MP2Node::check_request(){
	for(auto p = undone.begin();p!= undone.end();){
		if(p->second->replies - p->second->quorum >= 2 || this->par->getcurrtime() - p->second->timestamp > 4) {
			log_fail(p->second);
			delete p->second;
			p = undone.erase(p);
			continue;
		}
		if(p->second->quorum >= 2) {
			log_succ(p->second);
			delete p->second;
			p = undone.erase(p);
			continue;
		}
		p++;
	}	
}
void MP2Node::log_fail(request * req) {
	switch (req->msg_Type) {
		case CREATE:
			log->logCreateFail(&memberNode->addr, 1, req->id, req->key, req->value);
			break;
		case READ:
			log->logReadFail(&memberNode->addr, 1, req->id, req->key);
			break;
		case UPDATE:
			log->logUpdateFail(&memberNode->addr, 1, req->id, req->key, req->value);
			break;
		case DELETE: 
			log->logDeleteFail(&memberNode->addr, 1, req->id, req->key);
			break;
	}
}
void MP2Node::log_succ(request * req) {
	switch (req->msg_Type)
		case CREATE: {
			log->logCreateSuccess(&memberNode->addr, 1, req->id, req->key, req->value);
			break;
		case READ: 
			log->logReadSuccess(&memberNode->addr, 1, req->id, req->key, req->value);
			break;
		case UPDATE: 
			log->logUpdateSuccess(&memberNode->addr, 1, req->id, req->key, req->value);
			break;
		case DELETE: 
			log->logDeleteSuccess(&memberNode->addr, 1, req->id, req->key);
	}
}
