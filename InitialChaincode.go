package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	sc "github.com/hyperledger/fabric/protos/peer"
)

//SmartContract Define SmartContract structure
type SmartContract struct {
}

//Peer defines ths structure of a peer
type Peer struct {
	PeerID       string `json:"PeerID"`
	PeerDecision string `json:"PeerDecision"`
}

//Transaction defines the structure of a transaction
type Transaction struct {
	TransactionID     string    `json:"TransactionID"`
	InvolvedPeers     []Peer    `json:"InvolvedPeers"`
	FinalDecision     string    `json:"FinalDecision"`
	TransactionExpire time.Time `json:"TransactionExpire"`
}

//PeerUpdateRequestModel Model to represent a request to update a peers decision
type PeerUpdateRequestModel struct {
	TransactionID string `json:"TransactionID"`
	PeerID        string `json:"PeerID"`
	Decision      string `json:"Decision"`
}

//FinalDecisionResponseModel Model to represent the response model for a final decision request
type FinalDecisionResponseModel struct {
	TransactionID string `json:"TransactionID"`
	FinalDecision string `json:"FinalDecision"`
}

//PeerModel represents a peer
type PeerModel struct {
	PeerID string `json:"PeerID"`
}

//const define constants for transaction states and general keys
const (
	PendingState       = "P"
	CommitState        = "C"
	AbortState         = "A"
	RegisteredPeersKey = "RegisteredPeers"
)

//Init initializes the chaincode
func (s *SmartContract) Init(APIstub shim.ChaincodeStubInterface) sc.Response {
	return shim.Success(nil)
}

//Invoke called by any org of the blockchain
func (s *SmartContract) Invoke(APIstub shim.ChaincodeStubInterface) sc.Response {

	function, args := APIstub.GetFunctionAndParameters()

	switch function {
	case "addTransaction":
		return s.addTransaction(APIstub, args)
	case "queryTransaction":
		return s.queryTransaction(APIstub, args)
	case "makePeerDecision":
		return s.makePeerDecision(APIstub, args)
	case "queryFinalDecision":
		return s.queryFinalDecision(APIstub, args)
	case "registerPeer":
		return s.registerPeer(APIstub, args)
	case "getRegisteredPeers":
		return s.getRegisteredPeers(APIstub)
	}
	//functions
	return shim.Error("Invalid function")
}

//queryTransaction queries an specific transaction
func (s *SmartContract) queryTransaction(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {

	//todo check if any node has not been completed in order to make decision of abort
	if len(args) != 1 {
		return shim.Error("Invalid number of arguments. Expecting 1")
	}

	trans, err := APIstub.GetState(args[0])

	if err != nil {
		return shim.Error("Error getting transaction")
	}

	if trans == nil {
		return shim.Error("Transaction does not exist")
	}

	return shim.Success(trans)
}

//addTransaction creates a new transaction for the blockchain
func (s *SmartContract) addTransaction(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {

	if len(args) <= 0 {
		return shim.Error("Invalid arguments")
	}

	var currentTrans Transaction

	if err := json.Unmarshal([]byte(args[0]), &currentTrans); err != nil {
		return shim.Error("Invalid parameter")
	}

	if currentTrans.TransactionID == "" || s.checkTransactionIDExistence(APIstub, currentTrans.TransactionID) {
		return shim.Error("Invalid or already existent transaction id")
	}

	if len(currentTrans.InvolvedPeers) <= 0 {
		return shim.Error("There are no peers involved in the transaction")
	}

	currentTrans.FinalDecision = PendingState
	currentTrans.TransactionExpire = time.Now().Add(time.Minute * time.Duration(5)).UTC()

	for index := range currentTrans.InvolvedPeers {

		currentTrans.InvolvedPeers[index].PeerDecision = PendingState
	}

	transactionBytes, marshalError := json.Marshal(currentTrans)

	if marshalError != nil {
		return shim.Error("Internal error while marshalling data")
	}

	APIstub.PutState(currentTrans.TransactionID, transactionBytes)

	return shim.Success(nil)
}

//makePeerDecision function to update the decision state for each peer
func (s *SmartContract) makePeerDecision(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {

	if len(args) <= 0 {
		return shim.Error("Invalid arguments")
	}

	currentTrans := PeerUpdateRequestModel{}

	if err := json.Unmarshal([]byte(args[0]), &currentTrans); err != nil {
		return shim.Error("Invalid parameter")
	}

	if currentTrans.TransactionID == "" || !s.checkTransactionIDExistence(APIstub, currentTrans.TransactionID) {
		return shim.Error("Invalid transactionId")
	}

	transByte, err := APIstub.GetState(currentTrans.TransactionID)

	if err != nil || transByte == nil {
		return shim.Error("Could not get transaction from persistent state")
	}

	transaction := Transaction{}

	if err := json.Unmarshal(transByte, &transaction); err != nil {
		return shim.Error("Internal error with unmarshaling of data")
	}

	if transaction.FinalDecision != "" && transaction.FinalDecision != PendingState {
		return shim.Success(nil)
	}

	if len(transaction.InvolvedPeers) <= 0 {
		return shim.Error("Invalid number of peers for transaction")
	}

	peerUpdated := false

	for index, elem := range transaction.InvolvedPeers {
		if elem.PeerID != currentTrans.PeerID {
			continue
		}

		elem.PeerDecision = currentTrans.Decision

		transaction.InvolvedPeers[index] = elem
		peerUpdated = true
		break
	}

	if !peerUpdated {
		return shim.Error("Peer could not be found")
	}

	if currentTrans.Decision == AbortState {
		transaction.FinalDecision = AbortState
		return shim.Success(nil)
	}

	decision, state := s.checkPeersVoted(transaction)

	if decision {
		transaction.FinalDecision = state
	}

	marshalledUpdate, marshallError := json.Marshal(transaction)

	if marshallError != nil {
		return shim.Error("Internal error while updating transaction")
	}

	APIstub.PutState(transaction.TransactionID, marshalledUpdate)

	return shim.Success(nil)
}

//checkPeersVoted validates whether the peers have finished voting
func (s *SmartContract) checkPeersVoted(tran Transaction) (bool, string) {

	if len(tran.InvolvedPeers) <= 0 {
		return false, ""
	}

	for _, peer := range tran.InvolvedPeers {

		if peer.PeerDecision != "" && peer.PeerDecision == AbortState {
			return true, AbortState
		} else if (peer.PeerDecision == "" || peer.PeerDecision == PendingState) && time.Now().UTC().After(tran.TransactionExpire) {
			return true, AbortState
		} else {
			return false, PendingState
		}
	}

	return true, CommitState
}

func (s *SmartContract) queryFinalDecision(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {
	if len(args) <= 0 {
		return shim.Error("Invalid parameter number")
	}

	transID := args[0]
	if !s.checkTransactionIDExistence(APIstub, transID) {
		return shim.Error("Transaction does not exist")
	}

	trans, err := APIstub.GetState(transID)

	if err != nil {
		return shim.Error("Internal error while getting the transaction")
	}

	scTrans := Transaction{}

	if unmarshalErr := json.Unmarshal(trans, &scTrans); unmarshalErr != nil {
		return shim.Error("Internal error while unmarshalling data")
	}

	if scTrans.TransactionID == "" {
		return shim.Error("Internal error while getting transaction")
	}

	decision, state := s.checkPeersVoted(scTrans)

	if decision {
		scTrans.FinalDecision = state

		marshalledUpdate, marshallError := json.Marshal(scTrans)

		if marshallError != nil {
			return shim.Error("Internal error while updating transaction")
		}

		APIstub.PutState(scTrans.TransactionID, marshalledUpdate)
	}

	var finalDecision = FinalDecisionResponseModel{TransactionID: scTrans.TransactionID, FinalDecision: scTrans.FinalDecision}

	finalDecisionBytes, marshalError := json.Marshal(finalDecision)

	if marshalError != nil {
		return shim.Error("Internal error while handling transaction")
	}

	return shim.Success(finalDecisionBytes)
}

//checkTransactionIDExistence used to check if a transaction already exists in the blockchain before adding it
func (s *SmartContract) checkTransactionIDExistence(APIstub shim.ChaincodeStubInterface, transactionID string) bool {

	transaction, _ := APIstub.GetState(transactionID)

	if transaction == nil {
		return false
	}

	return true
}

//registerPeer used to register a new peer into the list of peers registered in the commit process
func (s *SmartContract) registerPeer(stub shim.ChaincodeStubInterface, args []string) sc.Response {
	if len(args) <= 0 {
		return shim.Error("Invalid parameters")
	}

	currentPeers, err := stub.GetState(RegisteredPeersKey)

	if err != nil || currentPeers == nil {
		newPeers := []PeerModel{}
		newPeers = append(newPeers, PeerModel{PeerID: args[0]})
		marshalledPeer, marshalErr := json.Marshal(newPeers)

		if marshalErr != nil {
			return shim.Error("Error while handling registration")
		}

		stub.PutState(RegisteredPeersKey, marshalledPeer)
	} else {
		unmarshalled := []PeerModel{}

		if unmarshallErr := json.Unmarshal(currentPeers, &unmarshalled); unmarshallErr != nil {
			return shim.Error("Error while retrieving data")
		}

		unmarshalled = append(unmarshalled, PeerModel{PeerID: args[0]})

		updatedMarshal, updatedErr := json.Marshal(unmarshalled)

		if updatedErr != nil {
			return shim.Error("Error while inserting peer")
		}

		stub.PutState(RegisteredPeersKey, updatedMarshal)
	}

	return shim.Success(nil)
}

func (s *SmartContract) getRegisteredPeers(stub shim.ChaincodeStubInterface) sc.Response {

	peers, err := stub.GetState(RegisteredPeersKey)

	if err != nil {
		return shim.Error("Error retrieving the list of peers")
	}

	return shim.Success(peers)
}

func main() {

	if err := shim.Start(new(SmartContract)); err != nil {
		fmt.Printf("Error creating the new Smart Contract: %s", err)
	}
}
