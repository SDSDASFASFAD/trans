import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import com.ilotterytech.ocean.dp.D1D2.beans.LotteryConsume;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameCategoryDecoder;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameCategoryFactory;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameFactory;
import com.ilotterytech.ocean.dp.D1D2.suicai.SuicaiGameFactoryFactory;
import com.ilotterytech.ocean.dp.D1D2.suicai.utils.ScTickerUtils;

import java.text.ParseException;

//public class test {
//    public static void main(String[] args) {
//        SuicaiGameFactoryFactory gameFactoryFactory = SuicaiGameFactoryFactory.getInstance();
//        GameFactory gameFactory = gameFactoryFactory.getFactory("10001");
//        GameCategoryFactory game = gameFactory.getCategoryFactory();
//        String content = ScTickerUtils.appendBrd("bettyp:3000;multiple:1;system:6;board:01 12 17 27 30 33 - 03",
//                "bettyp:3000;multiple:1;system:6;board:01 12 17 27 30 33 - 03");
//        String[] contents = game.decodeContent(content);
//        for (int i = 0 ; i < contents.length ; i++){
//            String prefix = game.decodeCategoryPrefix(contents[i]);
//            GameCategoryDecoder decoder = game.getCategoryDecoder(prefix);
//            LotteryConsume decode = decoder.decode(contents[i], new LotteryConsume());
//        }

//        String message = "*********";

//        try {
//            DateTime parse = DateUtil.parse(" 2022-02-02 ", "yyyy-MM-dd");
//        }catch (Exception e){
//            message = "站点设立时间数据格式不正确" ;
//        }
//        System.out.println(message);
//
//    }

//}
