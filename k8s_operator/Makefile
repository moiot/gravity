default: mock

mock:
	GOARCH=amd64 GOOS=linux go build -o mock_gravity/mock-linux mock_gravity/main.go
	docker build -f mock_gravity/Dockerfile -t huming/mock-gravity mock_gravity
	docker push huming/mock-gravity