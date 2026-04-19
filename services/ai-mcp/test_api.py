"""
LongForm Factory AI-MCP Service API 테스트 스크립트
"""

import requests
import json
import os
from typing import Dict, Any

# 설정
BASE_URL = os.getenv("API_URL", "http://localhost:8010")
API_KEY = os.getenv("LF_API_KEY", "test-key")
HEADERS = {
    "X-LF-API-Key": API_KEY,
    "Content-Type": "application/json",
}


def print_response(title: str, response: Dict[str, Any]) -> None:
    """응답 출력"""
    print(f"\n{'='*60}")
    print(f"[{title}]")
    print(f"{'='*60}")
    print(json.dumps(response, indent=2, ensure_ascii=False))


def test_health() -> bool:
    """헬스 체크"""
    try:
        resp = requests.get(f"{BASE_URL}/health")
        resp.raise_for_status()
        print_response("HEALTH CHECK", resp.json())
        return resp.status_code == 200
    except Exception as e:
        print(f"[ERROR] 헬스 체크 실패: {e}")
        return False


def test_generate() -> bool:
    """일반 생성 테스트"""
    try:
        payload = {
            "prompt": "AI와 영상 제작의 미래에 대해 200단어로 작성해주세요",
            "provider": "claude",
            "max_tokens": 500,
            "temperature": 0.7,
            "content_type": "shortform",
            "topic": "AI와 미디어",
        }
        
        resp = requests.post(
            f"{BASE_URL}/generate",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        print_response("POST /generate", resp.json())
        return resp.status_code == 200
    except Exception as e:
        print(f"[ERROR] 생성 테스트 실패: {e}")
        return False


def test_script_generation() -> bool:
    """스크립트 생성 테스트"""
    try:
        payload = {
            "topic": "우주 탐사의 최신 기술",
            "duration_seconds": 300,
            "style": "educational",
            "language": "ko",
            "target_audience": "일반인",
            "provider": "claude",
            "temperature": 0.7,
        }
        
        resp = requests.post(
            f"{BASE_URL}/generate/script",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        print_response("POST /generate/script", data)
        return resp.status_code == 200 and data.get("success")
    except Exception as e:
        print(f"[ERROR] 스크립트 생성 테스트 실패: {e}")
        return False


def test_title_generation() -> bool:
    """제목 생성 테스트"""
    try:
        payload = {
            "topic": "AI 역사",
            "style": "entertaining",
            "language": "ko",
            "provider": "claude",
        }
        
        resp = requests.post(
            f"{BASE_URL}/generate/title",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        print_response("POST /generate/title", data)
        return resp.status_code == 200 and len(data.get("titles", [])) > 0
    except Exception as e:
        print(f"[ERROR] 제목 생성 테스트 실패: {e}")
        return False


def test_description_generation() -> bool:
    """설명글 생성 테스트"""
    try:
        payload = {
            "title": "AI 역사 완벽 가이드",
            "topic": "인공지능 발전 과정",
            "duration_seconds": 600,
            "include_hashtags": True,
            "provider": "claude",
        }
        
        resp = requests.post(
            f"{BASE_URL}/generate/description",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        print_response("POST /generate/description", resp.json())
        return resp.status_code == 200
    except Exception as e:
        print(f"[ERROR] 설명글 생성 테스트 실패: {e}")
        return False


def test_tags_generation() -> bool:
    """태그 생성 테스트"""
    try:
        payload = {
            "topic": "AI 역사",
            "content_type": "video",
            "language": "ko",
            "count": 10,
            "provider": "claude",
        }
        
        resp = requests.post(
            f"{BASE_URL}/generate/tags",
            headers=HEADERS,
            json=payload,
        )
        resp.raise_for_status()
        data = resp.json()
        print_response("POST /generate/tags", data)
        return resp.status_code == 200 and len(data.get("tags", [])) > 0
    except Exception as e:
        print(f"[ERROR] 태그 생성 테스트 실패: {e}")
        return False


def main():
    """메인 테스트 함수"""
    print(f"""
╔══════════════════════════════════════════════════════════╗
║     LongForm Factory AI-MCP Service API Test             ║
║                    v2.0.0                                 ║
╚══════════════════════════════════════════════════════════╝

Service URL: {BASE_URL}
API Key: {API_KEY[:10]}... (configured)
""")

    tests = [
        ("헬스 체크", test_health),
        ("일반 생성", test_generate),
        ("스크립트 생성", test_script_generation),
        ("제목 생성", test_title_generation),
        ("설명글 생성", test_description_generation),
        ("태그 생성", test_tags_generation),
    ]

    results = {}
    for test_name, test_func in tests:
        print(f"\n[진행 중] {test_name}...")
        results[test_name] = test_func()

    # 결과 요약
    print(f"\n{'='*60}")
    print("[테스트 결과 요약]")
    print(f"{'='*60}")
    
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✓ PASS" if result else "✗ FAIL"
        print(f"{status:8} {test_name}")
    
    print(f"\n총계: {passed}/{total} 통과")
    
    if passed == total:
        print("\n모든 테스트가 성공했습니다! 서비스 준비 완료.")
    else:
        print(f"\n{total - passed}개 테스트 실패. 설정을 확인하세요.")
    
    return passed == total


if __name__ == "__main__":
    import sys
    success = main()
    sys.exit(0 if success else 1)
