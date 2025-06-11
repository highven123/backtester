import streamlit as st

st.set_page_config(page_title="数学学习路径评估", layout="centered")
st.title("🧠 数学学习路径评估（适合2~3年级孩子）")
st.write("请鼓励孩子独立完成以下题目，系统将自动评分并推荐学习方向。")
st.markdown("---")

score = 0

def ask_question(question, options, answer):
    global score
    user_answer = st.radio(question, options, key=question)
    if user_answer == answer:
        score += 1

# 初级题
st.subheader("🍎 初级题（1~4）：基础数感与直观思维")
ask_question("1. 8 + 5 = （ ）", ["A. 11", "B. 12", "C. 13", "D. 14"], "C. 13")
ask_question("2. 一根绳子剪成4段，每段长5厘米，这根绳子一共多长？", ["A. 9", "B. 15", "C. 20", "D. 25"], "C. 20")
ask_question("3. 小明看到2只鸟、3只猫，他看到多少只眼睛和脚？", ["A. 20眼 20脚", "B. 10眼 14脚", "C. 8眼 12脚", "D. 6眼 10脚"], "B. 10眼 14脚")
ask_question("4. 下面哪组图形是从小到大排列的？", ["A. ⬛⬜⬜⬜", "B. ⬜⬜⬜⬛", "C. ⬜⬛⬛⬛", "D. ⬛⬛⬛⬛"], "A. ⬛⬜⬜⬜")

# 中级题
st.subheader("🔍 中级题（5~8）：规律与逻辑")
ask_question("5. 数列：3，6，9，12，…，下一个数是？", ["A. 14", "B. 15", "C. 16", "D. 18"], "B. 15")
ask_question("6. 小红比小蓝高5厘米，小蓝比小绿高3厘米，小红比小绿高几厘米？", ["A. 2", "B. 5", "C. 8", "D. 15"], "C. 8")
ask_question("7. 一块蛋糕平均分成4份，3人每人吃一份，还剩几份？", ["A. 1", "B. 2", "C. 0", "D. 3"], "A. 1")
ask_question("8. 每个正方形有4条边，5个正方形有多少条边？", ["A. 20", "B. 16", "C. 24", "D. 10"], "A. 20")

# 高级题
st.subheader("🚀 拔高题（9~12）：初阶奥数思维")
ask_question("9. 一个数比25大，比40小，同时又是3的倍数，它可能是多少？", ["A. 24", "B. 30", "C. 42", "D. 45"], "B. 30")
ask_question("10. 桌上有5只苹果，小明吃了2只，又放回1只，现在桌上有几只？", ["A. 3", "B. 4", "C. 5", "D. 6"], "C. 5")
ask_question("11. 一个数加上6再减去4等于13，这个数是多少？", ["A. 11", "B. 12", "C. 13", "D. 14"], "A. 11")
ask_question("12. 一个长方形长8厘米，宽比长短3厘米，它的周长是？", ["A. 22", "B. 26", "C. 28", "D. 30"], "C. 28")

# 评分与建议
st.markdown("---")
if st.button("📝 查看评估结果"):
    st.subheader(f"🎯 总得分：{score} / 12")

    if score <= 4:
        st.warning("孩子目前适合继续思维启蒙课程（如豌豆思维），以培养数学兴趣为主。")
    elif 5 <= score <= 8:
        st.info("孩子已具备一定逻辑能力，可以逐步过渡到奥数启蒙体系，例如希望杯预备班等。")
    elif score >= 9:
        st.success("孩子展现出较强的数学理解与推理能力，建议系统进入奥数体系，打好竞赛基础！")

    st.markdown("如需详细学习建议或推荐课程资源，可继续咨询我！")