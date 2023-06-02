## bastion host (superset)
- private 네트워크에 대한 액세스 제공
- superset을 띄우기 위한 ec2 인스턴스


### bastion host에서 redshift serverless 클러스터 접속 

1. ec2 인스턴스 연결

```
ssh -i <YOUR_KEY_PAIR_FILE.pem> <USER_NAME>@<AWS_PUBLIC_DNS>
```
2. Redshift client 도구 설치

```
sudo apt install postgresql-client
```
3. psql 버전 확인
```
psql --version
```

4. 연결
```
psql -h <redshift-endpoint> -p <port> -U <username> -d <database-name>

```

