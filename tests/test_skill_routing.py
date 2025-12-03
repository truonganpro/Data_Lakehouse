# Test skill routing logic
from skills.distribution_region import DistributionRegionSkill
from skills.revenue_timeseries import RevenueTimeseriesSkill

# Test questions
test_questions = [
    "Tổng số đơn hàng và tổng doanh thu theo bang (geolocation_state) trong quý 3 năm 2018",
    "Phân bố doanh thu theo bang trong năm 2017",
    "Doanh thu theo tháng trong quý 3 năm 2018",
    "Doanh thu theo bang trong năm 2018",
    "Tổng doanh thu theo bang trong quý 3 năm 2018",
]

dist_skill = DistributionRegionSkill()
revenue_skill = RevenueTimeseriesSkill()

print("="*80)
print("TESTING SKILL ROUTING LOGIC")
print("="*80)

for q in test_questions:
    print(f"\nQ: {q}")
    dist_score = dist_skill.match(q, {})
    revenue_score = revenue_skill.match(q, {})
    print(f"   DistributionRegionSkill: {dist_score:.2f}")
    print(f"   RevenueTimeseriesSkill:  {revenue_score:.2f}")
    
    if dist_score > revenue_score:
        print(f"   ✅ Winner: DistributionRegionSkill")
    elif revenue_score > dist_score:
        print(f"   ✅ Winner: RevenueTimeseriesSkill")
    else:
        print(f"   ⚠️  Tie (both {dist_score:.2f})")

