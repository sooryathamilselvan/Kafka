#include <iostream>
#include <winsock2.h>

#pragma comment(lib, "ws2_32.lib")

using namespace std;

int main() {

    // 1. Start Winsock
    WSADATA wsaData;

    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        cout << "WSAStartup failed" << endl;
        return 1;
    }

    cout << "Winsock initialized" << endl;


    // 2. Create socket
    SOCKET clientSocket = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (clientSocket == INVALID_SOCKET) {
        cout << "Socket creation failed" << endl;
        WSACleanup();
        return 1;
    }

    cout << "Client socket created" << endl;


    // 3. Server address
    sockaddr_in serverAddress{};

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_port = htons(8080);
    serverAddress.sin_addr.s_addr = inet_addr("127.0.0.1");


    // 4. Connect
    if (connect(
        clientSocket,
        (sockaddr*)&serverAddress,
        sizeof(serverAddress)
    ) == SOCKET_ERROR) {

        cout << "Connection failed" << endl;

        closesocket(clientSocket);
        WSACleanup();

        return 1;
    }

    cout << "Connected to server!" << endl;


    // 5. Continuous communication
    string message;
    char buffer[1024];

    while (true) {

        cout << "\nEnter message: ";
        getline(cin, message);


        // Send message
        send(
            clientSocket,
            message.c_str(),
            message.length(),
            0
        );


        // Stop if exit
        if (message == "exit") {
            break;
        }


        // Receive response
        memset(buffer, 0, sizeof(buffer));

        int bytesReceived = recv(
            clientSocket,
            buffer,
            sizeof(buffer) - 1,
            0
        );

        if (bytesReceived <= 0) {
            cout << "Server disconnected." << endl;
            break;
        }

        buffer[bytesReceived] = '\0';

        cout << "Server says: "
             << buffer
             << endl;
    }


    // 6. Close
    closesocket(clientSocket);

    WSACleanup();

    cout << "Client closed." << endl;

    return 0;
}