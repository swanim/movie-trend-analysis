# Bastion host (superset)
목적
- superset을 띄우기 위한 ec2 인스턴스
- VPC 외부에 있는 redshift와 연결
- public 네트워크에서 private 네트워크에 대한 액세스 제공

### bastion host에서 redshift 연결 방법 

1. ec2 인스턴스(bastion host) 접속
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

4. ec2 인스턴스에서 Redshift 클러스터 연결
```
psql -h <redshift-endpoint> -p <port> -U <username> -d <database-name>
```

