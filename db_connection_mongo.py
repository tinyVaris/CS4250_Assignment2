#-------------------------------------------------------------------------
# AUTHOR: Isabel Ganda
# FILENAME: db_connection_mongo.py
# SPECIFICATION: Connects to given database and allows user to do CRUD operations on them via PyMongo.
# FOR: CS 4250-01 Assignment #2
# TIME SPENT: 2-3 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

from pymongo import MongoClient
import string

def connectDataBase():
    #Create a database connection object
    client = MongoClient('mongodb://localhost:27017/')
    db = client['pyMongoDatabase']
    return db

def createDocument(collection, docId, docText, docTitle, docDate, docCategory):
    #Used to remove any punctuations in text
    translator = str.maketrans('', '', string.punctuation)

    #Create a 'document' using dictionaries
    terms = docText.translate(translator).lower().split(" ")
    termCounts = {}
    
    for term in terms:
        termCounts[term] = termCounts.get(term, 0) + 1
    
    #Create a list of dictionaries with term occurrences and their character counts
    termList = [{'term': term, 'count': count, 'num_chars': len(term)} for term, count in termCounts.items()]
    
    #Producing a final document as a dictionary including all the required fields
    document = {
        'id': docId,
        'title': docTitle,
        'date': docDate,
        'category': docCategory,
        'terms': termList
    }

    #Insert the document
    collection.insert_one(document)

#Delete doucment function
def deleteDocument(collection, docId):
    collection.delete_one({'id': docId})

#Delete the 'old' document, and replace it with the 'updated' document
def updateDocument(collection, docId, docText, docTitle, docDate, docCategory):
    deleteDocument(collection, docId)
    createDocument(collection, docId, docText, docTitle, docDate, docCategory)

#Get the index of the collection
def getIndex(collection):
    invertedIndex = {}
    
    #retrieve all documents in the collection
    documents = collection.find()

    #Iterate through all documents
    for document in documents:
        #Get the title of the current doc
        title = document['title']

        #Iterate through the current document terms
        for termInfo in document['terms']:
            term = termInfo['term']
            count = termInfo['count']

            #Format term and count into appropriate string
            entry = f"{title}:{count}"

            #If term is not in the invertedIndex
            if term not in invertedIndex:
                invertedIndex[term] = [entry] 
            
            #If term is already present, check if the entry is present
            elif entry not in invertedIndex[term]:
                invertedIndex[term].append(entry)
                

    # Format index
    formattedIndex = {term: ','.join(entries) for term, entries in invertedIndex.items()}
    
    return formattedIndex
