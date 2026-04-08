import asyncio
import json
import os
from dotenv import load_dotenv
from edgex_sdk import Client

async def main():
    load_dotenv("arb.env")
    base_url = os.getenv('EDGEX_BASE_URL', 'https://pro.edgex.exchange')
    account_id = os.getenv('EDGEX_ACCOUNT_ID', '1')
    stark_key = os.getenv('EDGEX_STARK_PRIVATE_KEY', '0x1')
    
    client = Client(base_url=base_url, account_id=int(account_id), stark_private_key=stark_key)
    try:
        res = await client.get_metadata()
        data = res.get('data', {})
        contract_list = data.get('contractList', [])
        print(f"Total contracts: {len(contract_list)}")
        for c in contract_list:
            print(f"Symbol: {c.get('symbol')}, ContractId: {c.get('contractId')}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.close()

if __name__ == "__main__":
    asyncio.run(main())
