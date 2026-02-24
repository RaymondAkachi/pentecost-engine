import os
import shutil
import asyncio
from temporalio.client import Client
from temporalio import workflow
from datetime import timedelta

@workflow.defn
class VideoBypassWorkflow:
    @workflow.run
    async def run(self, thumbnail_dir: str) -> dict:
        return await workflow.execute_activity(
            "DetectFaces",
            thumbnail_dir,
            start_to_close_timeout=timedelta(seconds=15),
        )

async def main():
    test_dir = "/shared/chunks/thumbs_test"
    os.makedirs(test_dir, exist_ok=True)
    
    ref_dir = "/app/reference_gallery"
    print(f"📂 Preparing test data in {test_dir}...")
    
    if os.path.exists(ref_dir):
        images = [f for f in os.listdir(ref_dir) if f.endswith(('.jpg', '.jpeg'))]
        for i, img in enumerate(images[:15]): 
            shutil.copy(os.path.join(ref_dir, img), os.path.join(test_dir, f"frame_{i:02d}.jpg"))
        print(f"✅ Copied {min(len(images), 15)} test frames into RAM disk.")
    else:
        print("⚠️ Warning: Could not find reference gallery.")

    # FIX: Self-Healing Connection Loop
    print("🔄 Connecting to Temporal at temporal:7233...")
    while True:
        try:
            client = await Client.connect("temporal:7233")
            break
        except Exception as e:
            print(f"⏳ Waiting for Temporal server: {e}")
            await asyncio.sleep(2)
            
    print("🚀 Firing test payload to Temporal Worker...")
    
    result = await client.execute_workflow(
        VideoBypassWorkflow.run,
        test_dir, 
        id="test-bypass-1",
        task_queue="PENTECOST_TASK_QUEUE",
    )
    
    print("\n==================================================")
    print(f"🎯 DECISION RESULT: {result}")
    print("==================================================\n")

if __name__ == "__main__":
    asyncio.run(main())