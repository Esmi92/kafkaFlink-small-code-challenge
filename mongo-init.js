db = db.getSiblingDB("clientData")

const data = [
    { _id: 1, deviceId: "dev-001", phoneNumber: "+52-555-111-2222", email: "client1@example.com" }, 
    { _id: 2, deviceId: "dev-002", phoneNumber: "+52-555-333-4444", email: "client2@example.com" }, 
    { _id: 3, deviceId: "dev-003", phoneNumber: "+52-555-555-6666", email: "client3@example.com" }, 
    { _id: 4, deviceId: "dev-004", phoneNumber: "+52-555-777-8888", email: "client4@example.com" }, 
    { _id: 5, deviceId: "dev-005", phoneNumber: "+52-555-999-0000", email: "client5@example.com" }
]

db.clients.insertMany(data)