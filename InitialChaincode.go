package main

import (
	"encoding/json"
	"fmt"

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
	TransactionID string `json:"TransactionID"`
	InvolvedPeers []Peer `json:"InvolvedPeers"`
	FinalDecision string `json:"FinalDecision"`
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

//Init initializes the ledger
func (s *SmartContract) Init(APIstub shim.ChaincodeStubInterface) sc.Response {
	return shim.Success(nil)
}

//Invoke called by any org of the blockchain
func (s *SmartContract) Invoke(APIstub shim.ChaincodeStubInterface) sc.Response {

	function, args := APIstub.GetFunctionAndParameters()

	switch function {
	case "addTransaction":
		s.addTransaction(APIstub, args)
	case "queryTransaction":
		s.queryTransaction(APIstub, args)
	case "makePeerDecision":
		s.makePeerDecision(APIstub, args)
	case "queryFinalDecision":
		s.queryFinalDecision(APIstub, args)
	}
	//functions
	return shim.Error("Invalid function")
}

//queryTransaction queries an specific transaction
func (s *SmartContract) queryTransaction(APIstub shim.ChaincodeStubInterface, args []string) sc.Response {

	if len(args) != 1 {
		return shim.Error("Invalid number of arguments. Expecting 1")
	}

	trans, err := APIstub.GetState(args[0])

	if err != nil {
		return shim.Error("Error getting transaction")
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

	var currentTrans PeerUpdateRequestModel

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

	marshalledUpdate, marshallError := json.Marshal(transaction)

	if marshallError != nil {
		return shim.Error("Internal error while updating transaction")
	}

	APIstub.PutState(transaction.TransactionID, marshalledUpdate)

	return shim.Success(nil)
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
		return true
	}

	return false
}

func main() {
	err := shim.Start(new(SmartContract))

	if err != nil {
		fmt.Printf("Error creating the new Smart Contract: %s", err)
	}
}
