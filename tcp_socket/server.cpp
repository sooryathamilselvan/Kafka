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
    SOCKET serverSocket = socket(
        AF_INET,
        SOCK_STREAM,
        0
    );

    if (serverSocket == INVALID_SOCKET) {
        cout << "Socket creation failed" << endl;
        WSACleanup();
        return 1;
    }

    cout << "Server socket created" << endl;


    // 3. Configure server address
    sockaddr_in serverAddress{};

    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(8080);


    // 4. Bind socket
    if (bind(
        serverSocket,
        (sockaddr*)&serverAddress,
        sizeof(serverAddress)
    ) == SOCKET_ERROR) {

        cout << "Bind failed" << endl;

        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }

    cout << "Socket bound to port 8080" << endl;


    // 5. Listen
    if (listen(serverSocket, 5) == SOCKET_ERROR) {

        cout << "Listen failed" << endl;

        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }

    cout << "Server listening..." << endl;


    // 6. Accept client
    SOCKET clientSocket = accept(
        serverSocket,
        nullptr,
        nullptr
    );

    if (clientSocket == INVALID_SOCKET) {

        cout << "Accept failed" << endl;

        closesocket(serverSocket);
        WSACleanup();

        return 1;
    }

    cout << "Client connected!" << endl;


    // 7. Continuous communication
    char buffer[1024];

    while (true) {

        memset(buffer, 0, sizeof(buffer));

        int bytesReceived = recv(
            clientSocket,
            buffer,
            sizeof(buffer) - 1,
            0
        );

        // Client disconnected
        if (bytesReceived <= 0) {
            cout << "Client disconnected." << endl;
            break;
        }

        buffer[bytesReceived] = '\0';

        cout << "Client says: "
             << buffer
             << endl;


        // Stop if client sends exit
        if (strcmp(buffer, "exit") == 0) {
            cout << "Client requested to close." << endl;
            break;
        }


        // Send response
        string response = "Server received: " + string(buffer);

        send(
            clientSocket,
            response.c_str(),
            response.length(),
            0
        );
    }


    // 8. Close sockets
    closesocket(clientSocket);
    closesocket(serverSocket);

    WSACleanup();

    cout << "Server closed." << endl;

    return 0;
}