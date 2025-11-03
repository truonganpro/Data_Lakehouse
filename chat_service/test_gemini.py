#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Test script for Gemini integration
Run this to verify Gemini API is working correctly
"""
import os
import sys


def test_gemini_embeddings():
    """Test Gemini embeddings"""
    print("\n" + "="*60)
    print("🧪 TEST 1: Gemini Embeddings")
    print("="*60)
    
    try:
        from embeddings import embed_texts
        
        # Set provider to Gemini
        os.environ["EMBEDDING_PROVIDER"] = "gemini"
        
        test_texts = [
            "Doanh thu theo tháng gần đây?",
            "Top 10 sản phẩm bán chạy nhất?"
        ]
        
        print(f"📝 Embedding {len(test_texts)} texts with Gemini...")
        embeddings, dim = embed_texts(test_texts)
        
        print(f"✅ Success!")
        print(f"   - Generated {len(embeddings)} embeddings")
        print(f"   - Dimension: {dim}")
        print(f"   - First embedding shape: {len(embeddings[0])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_gemini_sql_generation():
    """Test Gemini SQL generation"""
    print("\n" + "="*60)
    print("🧪 TEST 2: Gemini SQL Generation")
    print("="*60)
    
    try:
        from llm_sql import gen_sql_with_gemini
        
        # Set provider to Gemini
        os.environ["LLM_PROVIDER"] = "gemini"
        
        test_questions = [
            "Doanh thu theo tháng 3 tháng gần đây?",
            "Phương thức thanh toán nào phổ biến nhất?",
            "Top 10 sản phẩm bán chạy 6 tháng qua?"
        ]
        
        success_count = 0
        
        for i, question in enumerate(test_questions, 1):
            print(f"\n{i}. Testing: {question}")
            sql = gen_sql_with_gemini(question)
            
            if sql:
                print(f"   ✅ Generated SQL ({len(sql)} chars)")
                print(f"   Preview: {sql[:150]}...")
                success_count += 1
            else:
                print(f"   ❌ Failed to generate SQL")
        
        print(f"\n📊 Results: {success_count}/{len(test_questions)} successful")
        
        return success_count == len(test_questions)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def test_gemini_summarization():
    """Test Gemini summarization"""
    print("\n" + "="*60)
    print("🧪 TEST 3: Gemini Summarization")
    print("="*60)
    
    try:
        from llm_summarize import summarize_with_gemini
        
        # Set provider to Gemini
        os.environ["LLM_PROVIDER"] = "gemini"
        
        test_question = "Doanh thu theo tháng 3 tháng gần đây?"
        test_data = [
            {"month": "2018-08-01", "revenue": 1234567.89, "orders": 5432},
            {"month": "2018-07-01", "revenue": 1123456.78, "orders": 4987},
            {"month": "2018-06-01", "revenue": 987654.32, "orders": 4123},
        ]
        
        print(f"📝 Summarizing results for: {test_question}")
        summary = summarize_with_gemini(test_question, test_data, None)
        
        if summary:
            print(f"✅ Generated summary:")
            print(f"\n{summary}\n")
            return True
        else:
            print(f"❌ No summary generated")
            return False
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


def main():
    """Run all tests"""
    print("\n" + "="*60)
    print("🚀 GEMINI INTEGRATION TEST SUITE")
    print("="*60)
    
    # Check for API key
    api_key = os.getenv("GOOGLE_API_KEY")
    if not api_key:
        print("\n❌ ERROR: GOOGLE_API_KEY not set!")
        print("\nPlease set it in one of these ways:")
        print("  1. export GOOGLE_API_KEY='your-api-key'")
        print("  2. Add to .env file: GOOGLE_API_KEY=your-api-key")
        print("\n💡 Get your API key from: https://makersuite.google.com/app/apikey")
        sys.exit(1)
    
    print(f"\n✅ GOOGLE_API_KEY found: {api_key[:10]}...{api_key[-5:]}")
    
    # Run tests
    results = []
    
    results.append(("Gemini Embeddings", test_gemini_embeddings()))
    results.append(("Gemini SQL Generation", test_gemini_sql_generation()))
    results.append(("Gemini Summarization", test_gemini_summarization()))
    
    # Summary
    print("\n" + "="*60)
    print("📊 TEST SUMMARY")
    print("="*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed! Gemini integration is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed. Please check the errors above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())

