"""
FastAPI TTS 서비스 테스트 클라이언트
로컬 개발/테스트 용도
"""

import asyncio
import json
import sys
from pathlib import Path

import httpx

# ==================== 설정 ====================

BASE_URL = "http://localhost:8001"
TEST_OUTPUT_DIR = Path("test_outputs")
TEST_OUTPUT_DIR.mkdir(exist_ok=True)

# ==================== 테스트 케이스 ====================


async def test_health():
    """헬스 체크 테스트"""
    print("\n[TEST 1] 헬스 체크")
    print("-" * 50)

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{BASE_URL}/health")

            print(f"상태 코드: {response.status_code}")
            data = response.json()
            print(f"응답: {json.dumps(data, indent=2, ensure_ascii=False)}")

            assert response.status_code == 200
            assert data["status"] == "healthy"
            print("✅ 헬스 체크 통과")
            return True

    except Exception as e:
        print(f"❌ 헬스 체크 실패: {e}")
        return False


async def test_voices():
    """음성 목록 조회 테스트"""
    print("\n[TEST 2] 음성 목록 조회")
    print("-" * 50)

    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(f"{BASE_URL}/voices")

            print(f"상태 코드: {response.status_code}")
            data = response.json()

            if not data:
                print("⚠️  음성 목록이 비어있습니다 (API 연결 확인 필요)")
                return False

            print(f"사용 가능한 음성: {len(data)}개")
            for i, voice in enumerate(data[:3], 1):
                print(f"  {i}. {voice.get('name')} (ID: {voice.get('voice_id')})")

            assert response.status_code == 200
            print("✅ 음성 목록 조회 통과")
            return True

    except Exception as e:
        print(f"❌ 음성 목록 조회 실패: {e}")
        return False


async def test_tts_korean_male():
    """한국어 남성 음성 TTS 테스트"""
    print("\n[TEST 3] TTS 변환 (한국어 남성)")
    print("-" * 50)

    try:
        payload = {
            "text": "안녕하세요. 롱폼 팩토리 TTS 서비스입니다.",
            "voice_preset": "korean_male",
            "stability": 0.5,
            "similarity_boost": 0.75,
            "filename": "test_korean_male"
        }

        print(f"요청: {json.dumps(payload, indent=2, ensure_ascii=False)}")

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{BASE_URL}/tts",
                json=payload
            )

            print(f"상태 코드: {response.status_code}")
            data = response.json()
            print(f"응답: {json.dumps(data, indent=2, ensure_ascii=False)}")

            assert response.status_code == 200
            assert data["success"] is True
            assert data["duration_seconds"] > 0

            print(f"✅ TTS 변환 통과")
            print(f"   파일: {data['file_path']}")
            print(f"   길이: {data['duration_seconds']:.2f}초")
            print(f"   문자수: {data['characters']}자")
            return True

    except Exception as e:
        print(f"❌ TTS 변환 실패: {e}")
        return False


async def test_tts_korean_female():
    """한국어 여성 음성 TTS 테스트"""
    print("\n[TEST 4] TTS 변환 (한국어 여성)")
    print("-" * 50)

    try:
        payload = {
            "text": "롱폼 팩토리에 오신 것을 환영합니다.",
            "voice_preset": "korean_female",
            "stability": 0.7,
            "similarity_boost": 0.8,
            "filename": "test_korean_female"
        }

        print(f"요청: {json.dumps(payload, indent=2, ensure_ascii=False)}")

        async with httpx.AsyncClient(timeout=60.0) as client:
            response = await client.post(
                f"{BASE_URL}/tts",
                json=payload
            )

            print(f"상태 코드: {response.status_code}")
            data = response.json()
            print(f"응답: {json.dumps(data, indent=2, ensure_ascii=False)}")

            assert response.status_code == 200
            assert data["success"] is True
            assert data["duration_seconds"] > 0

            print(f"✅ TTS 변환 통과")
            print(f"   파일: {data['file_path']}")
            print(f"   길이: {data['duration_seconds']:.2f}초")
            return True

    except Exception as e:
        print(f"❌ TTS 변환 실패: {e}")
        return False


async def test_batch_tts():
    """배치 TTS 테스트"""
    print("\n[TEST 5] 배치 TTS 변환")
    print("-" * 50)

    try:
        payload = {
            "items": [
                {
                    "text": "첫 번째 문장입니다.",
                    "voice_preset": "korean_male",
                    "filename": "batch_item_1"
                },
                {
                    "text": "두 번째 문장입니다.",
                    "voice_preset": "korean_female",
                    "filename": "batch_item_2"
                },
                {
                    "text": "세 번째 문장입니다.",
                    "voice_preset": "korean_male",
                    "filename": "batch_item_3"
                }
            ]
        }

        print(f"요청: {len(payload['items'])}개 항목 배치")

        async with httpx.AsyncClient(timeout=180.0) as client:
            response = await client.post(
                f"{BASE_URL}/tts/batch",
                json=payload
            )

            print(f"상태 코드: {response.status_code}")
            data = response.json()

            print(f"응답 요약:")
            print(f"  - 총 항목: {data['total']}")
            print(f"  - 성공: {data['success_count']}")

            for i, result in enumerate(data["results"], 1):
                status = "✅" if result["success"] else "❌"
                duration = f"{result['duration_seconds']:.2f}초" if result['success'] else "N/A"
                print(f"  {i}. {status} {result.get('message', duration)}")

            assert response.status_code == 200
            assert data["success_count"] > 0

            print(f"\n✅ 배치 TTS 통과")
            return True

    except Exception as e:
        print(f"❌ 배치 TTS 실패: {e}")
        return False


async def test_invalid_text():
    """유효하지 않은 입력 테스트"""
    print("\n[TEST 6] 에러 처리 (빈 텍스트)")
    print("-" * 50)

    try:
        payload = {
            "text": "",  # 빈 텍스트
            "voice_preset": "korean_male"
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BASE_URL}/tts",
                json=payload
            )

            print(f"상태 코드: {response.status_code}")

            assert response.status_code == 422  # Validation error
            print("✅ 에러 처리 통과")
            return True

    except Exception as e:
        print(f"❌ 에러 처리 테스트 실패: {e}")
        return False


# ==================== 메인 실행 ====================


async def main():
    """모든 테스트 실행"""
    print("=" * 60)
    print("FastAPI TTS 서비스 테스트 스위트")
    print("=" * 60)

    # 서버 연결 확인
    try:
        async with httpx.AsyncClient() as client:
            await client.get(f"{BASE_URL}/health", timeout=5.0)
    except Exception as e:
        print(f"\n❌ 서버에 연결할 수 없습니다: {e}")
        print(f"주소: {BASE_URL}")
        print("\n다음을 확인하세요:")
        print("1. Docker 컨테이너가 실행 중인가?")
        print("2. ELEVENLABS_API_KEY 환경 변수가 설정되었는가?")
        sys.exit(1)

    # 테스트 실행
    results = []
    results.append(("헬스 체크", await test_health()))
    results.append(("음성 목록", await test_voices()))
    results.append(("TTS (남성)", await test_tts_korean_male()))
    results.append(("TTS (여성)", await test_tts_korean_female()))
    results.append(("배치 TTS", await test_batch_tts()))
    results.append(("에러 처리", await test_invalid_text()))

    # 결과 요약
    print("\n" + "=" * 60)
    print("테스트 결과 요약")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")

    print("-" * 60)
    print(f"총 {passed}/{total} 테스트 통과")
    print("=" * 60)

    return 0 if passed == total else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
