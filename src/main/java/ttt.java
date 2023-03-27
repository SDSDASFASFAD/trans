import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.db.ds.simple.SimpleDataSource;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameCategoryDecoder;
import com.ilotterytech.ocean.dp.D1D2.interfaces.GameCategoryFactory;
import com.ilotterytech.ocean.dp.D1D2.suicai.SuicaiGameFactoryFactory;

public class ttt {
    public static void main(String[] args) {

//        Db db = DbUtil.use(new SimpleDataSource("jdbc:mysql://192.168.110.2:3306/cwlcore", "cwl", "cwl1234"));
        Db db = DbUtil.use(new SimpleDataSource("jdbc:mysql://localhost:3306/cwldataware?useSSL=false", "root", "root"));

    }
}
