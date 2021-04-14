# KAFKA Publisher
## 설치 방법
1. git 설치
```
https://git-scm.com/
```

2. adm 프로그램 설치
```
git clone https://gitlab.com/tp.rc.cyg/kafka_publisher.git
```

3. Python 3.8 설치
```
https://www.python.org/downloads/
```

4. 라이브러리 설치
- 파이썬 라이브러리
```
pip install -r requirements.txt
```

- 오라클 라이브러리
기존에 설치안했다면, 아래 링크에서 다운로드 받아 설치
```
https://www.oracle.com/kr/database/technologies/instant-client/winx64-64-downloads.html
```

5. kpm/examples 에 있는 kpm.cfg 를 수정하여서 특정 폴더에 저장
```
Ex) kpm/cfg/kpm.cfg
```

6. Unit-test 를 실행하여 문제 없는지 확인 (테스트 작성을 안했기 때문에, 패스)
```
Ex) tox
Ex) python setup.py test
Ex) python setup.py test --test-suite tests.test_util.TestUtil
Ex) python setup.py test --test-suite tests.test_util.TestUtil.test_to_int

```

7. 실행
```
# 옵션 파일이 kpm/cfg/에 위치할 경우
cd bin
python kp-manager.py --config=../cfg/kpm.cfg
```

8. 프로그램 종료
- `Ctrl + C` 로 종료해야 함 (현재 개발에서는 되지만 상용에서는 안됨)
- 오라클 sqlnet.ora 중,  DISABLE_OOB 가 'ON' 으로 설정되어 있을 경우, `Ctrl + C` 로 프로그램 종료가 안됨 
- DB 설정을 바꿀수 없는 상황이니, `Ctrl + Z` 로 프로그램을 나와서 프로세스는 직접 제거
    ```
    $ kill -9 `ps -ef | grep "python bin/kp-manager.py" | grep -v grep | awk '{print $2}'`
    ```