import os
from project.apps.chatgpt.handlers import write_proposal
from project.apps.chatgpt.handler_utils import _repo_root

def test_write_proposal_manual():
    content = "program_id: test_proposal\nsymbols: [BTCUSDT]"
    filename = "test_manual_write.yaml"
    directory = "project/configs/proposals"
    
    result = write_proposal(
        proposal_content=content,
        filename=filename,
        directory=directory
    )
    
    print(f"Result: {result}")
    
    full_path = _repo_root() / directory / filename
    assert full_path.exists()
    assert full_path.read_text() == content
    
    # Cleanup
    full_path.unlink()
    print("Verification successful and cleaned up.")

if __name__ == "__main__":
    try:
        test_write_proposal_manual()
    except Exception as e:
        print(f"Verification failed: {e}")
        import traceback
        traceback.print_exc()
