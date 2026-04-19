"""
FFmpeg Worker API 통합 테스트 스크립트

사용법:
    python test_api.py
    python test_api.py --job-id my_job_001
    python test_api.py --url http://localhost:8002
"""

import asyncio
import argparse
import json
import time
from typing import Dict, Any
from datetime import datetime

import httpx


class FFmpegWorkerClient:
    """FFmpeg Worker API 클라이언트"""
    
    def __init__(self, base_url: str = "http://localhost:8002", timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.client = None
    
    async def __aenter__(self):
        self.client = httpx.AsyncClient(base_url=self.base_url, timeout=self.timeout)
        return self
    
    async def __aexit__(self, *args):
        await self.client.aclose()
    
    async def health_check(self) -> Dict[str, Any]:
        """헬스 체크"""
        response = await self.client.get("/health")
        response.raise_for_status()
        return response.json()
    
    async def search_assets(self, job_id: str, scenes: list, sources: str = "pexels,pixabay") -> Dict[str, Any]:
        """자산 검색 및 다운로드"""
        payload = {
            "job_id": job_id,
            "scenes": scenes,
            "sources": sources
        }
        response = await self.client.post("/assets/search", json=payload)
        response.raise_for_status()
        return response.json()
    
    async def create_video(
        self,
        job_id: str,
        mode: str = "longform",
        resolution: str = "1920x1080",
        fps: int = 30,
        add_bgm: bool = True,
        bgm_volume: float = 0.3,
        generate_thumbnail: bool = True,
        generate_shorts: bool = True,
        title: str = None
    ) -> Dict[str, Any]:
        """영상 생성"""
        payload = {
            "job_id": job_id,
            "mode": mode,
            "resolution": resolution,
            "fps": fps,
            "add_bgm": add_bgm,
            "bgm_volume": bgm_volume,
            "generate_thumbnail": generate_thumbnail,
            "generate_shorts": generate_shorts
        }
        
        if title:
            payload["title"] = title
        
        response = await self.client.post("/video/create", json=payload)
        response.raise_for_status()
        return response.json()
    
    async def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """작업 상태 조회"""
        response = await self.client.get(f"/job/{job_id}/status")
        response.raise_for_status()
        return response.json()


async def test_health_check(client: FFmpegWorkerClient):
    """헬스 체크 테스트"""
    print("\n" + "="*60)
    print("테스트 1: 헬스 체크")
    print("="*60)
    
    try:
        result = await client.health_check()
        print("✓ 응답 성공")
        print(f"  서비스: {result['service']}")
        print(f"  버전: {result['version']}")
        print(f"  상태: {result['status']}")
        return True
    except Exception as e:
        print(f"✗ 오류: {e}")
        return False


async def test_asset_search(client: FFmpegWorkerClient, job_id: str):
    """자산 검색 테스트"""
    print("\n" + "="*60)
    print("테스트 2: 자산 검색 및 다운로드")
    print("="*60)
    
    scenes = [
        {
            "scene_id": "scene_1",
            "keyword": "nature landscape green forest",
            "duration_seconds": 3.0,
            "description": "초록색 숲 풍경"
        },
        {
            "scene_id": "scene_2",
            "keyword": "sunset ocean beach",
            "duration_seconds": 4.0,
            "description": "해변 일몰"
        },
        {
            "scene_id": "scene_3",
            "keyword": "city traffic urban",
            "duration_seconds": 2.5,
            "description": "도시 교통"
        }
    ]
    
    try:
        print(f"검색할 장면: {len(scenes)}개")
        result = await client.search_assets(job_id, scenes)
        
        print("✓ 자산 검색 완료")
        print(f"  다운로드 완료: {result['downloaded_count']}/{result['total_count']}")
        print(f"  상태: {result['status']}")
        
        for scene in result['scenes']:
            if scene.get('asset_url'):
                print(f"  ✓ {scene['scene_id']}: {scene['asset_url']}")
            else:
                print(f"  ✗ {scene['scene_id']}: 다운로드 실패")
        
        return True
    except Exception as e:
        print(f"✗ 오류: {e}")
        return False


async def test_video_creation(client: FFmpegWorkerClient, job_id: str):
    """영상 생성 테스트"""
    print("\n" + "="*60)
    print("테스트 3: 영상 생성 요청")
    print("="*60)
    
    try:
        result = await client.create_video(
            job_id=job_id,
            mode="longform",
            add_bgm=True,
            bgm_volume=0.3,
            generate_thumbnail=True,
            generate_shorts=True,
            title="Test Video"
        )
        
        print("✓ 영상 생성 요청 성공")
        print(f"  작업 ID: {result['job_id']}")
        print(f"  상태: {result['status']}")
        
        return True
    except Exception as e:
        print(f"✗ 오류: {e}")
        return False


async def wait_for_completion(
    client: FFmpegWorkerClient,
    job_id: str,
    check_interval: float = 5.0,
    max_wait: float = 300.0
):
    """작업 완료 대기"""
    print("\n" + "="*60)
    print("테스트 4: 작업 상태 모니터링")
    print("="*60)
    
    start_time = time.time()
    check_count = 0
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed > max_wait:
            print(f"\n✗ 타임아웃: {max_wait}초 경과")
            return False
        
        try:
            status = await client.get_job_status(job_id)
            check_count += 1
            
            print(f"\n[확인 #{check_count}] {status['status'].upper()}")
            print(f"  진행률: {status['progress']:.1f}%")
            print(f"  경과 시간: {elapsed:.0f}초")
            
            if status['error']:
                print(f"  에러: {status['error']}")
            
            if status['status'] in ['completed', 'failed', 'cancelled']:
                print("\n✓ 작업 완료")
                
                if status['output_files']:
                    print("  출력 파일:")
                    for file_type, file_path in status['output_files'].items():
                        print(f"    - {file_type}: {file_path}")
                
                if status['duration_seconds']:
                    print(f"  영상 길이: {status['duration_seconds']:.1f}초")
                
                return status['status'] == 'completed'
            
            await asyncio.sleep(check_interval)
        
        except Exception as e:
            print(f"✗ 상태 조회 오류: {e}")
            await asyncio.sleep(check_interval)


async def run_full_test(client: FFmpegWorkerClient, job_id: str):
    """전체 테스트 실행"""
    print("\n" + "╔" + "="*58 + "╗")
    print("║" + " "*10 + "FFmpeg Worker API 통합 테스트" + " "*18 + "║")
    print("║" + f" Job ID: {job_id}" + " "*(50-len(job_id)) + "║")
    print("║" + f" 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}" + " "*(25) + "║")
    print("╚" + "="*58 + "╝")
    
    # 1. 헬스 체크
    if not await test_health_check(client):
        print("\n서버에 연결할 수 없습니다. 서버가 실행 중인지 확인하세요.")
        return False
    
    # 2. 자산 검색
    if not await test_asset_search(client, job_id):
        print("\n자산 검색 실패")
        return False
    
    # 3. 영상 생성 요청
    if not await test_video_creation(client, job_id):
        print("\n영상 생성 요청 실패")
        return False
    
    # 4. 작업 완료 대기
    success = await wait_for_completion(client, job_id)
    
    # 최종 상태 확인
    status = await client.get_job_status(job_id)
    
    print("\n" + "="*60)
    print("최종 결과 요약")
    print("="*60)
    print(f"작업 ID: {job_id}")
    print(f"최종 상태: {status['status']}")
    print(f"성공 여부: {'✓ 성공' if success else '✗ 실패'}")
    
    if status['output_files']:
        print("\n생성된 파일:")
        for file_type, file_path in status['output_files'].items():
            print(f"  - {file_type}: {file_path}")
    
    if status['error']:
        print(f"\n에러 메시지:")
        print(f"  {status['error']}")
    
    print(f"\n종료: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    return success


async def main():
    parser = argparse.ArgumentParser(
        description="FFmpeg Worker API 테스트 스크립트"
    )
    parser.add_argument(
        "--url",
        default="http://localhost:8002",
        help="FFmpeg Worker 서비스 URL (기본: http://localhost:8002)"
    )
    parser.add_argument(
        "--job-id",
        default=None,
        help="작업 ID (기본값: test_job_{timestamp})"
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="HTTP 타임아웃 (초, 기본: 60)"
    )
    
    args = parser.parse_args()
    
    # 작업 ID 생성
    if not args.job_id:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.job_id = f"test_job_{timestamp}"
    
    print(f"\n서비스 URL: {args.url}")
    print(f"작업 ID: {args.job_id}")
    
    async with FFmpegWorkerClient(base_url=args.url, timeout=args.timeout) as client:
        success = await run_full_test(client, args.job_id)
        exit(0 if success else 1)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n테스트가 사용자에 의해 중단되었습니다.")
        exit(1)
    except Exception as e:
        print(f"\n예상 치 못한 오류: {e}")
        exit(1)
