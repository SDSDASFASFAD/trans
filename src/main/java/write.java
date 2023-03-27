import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import jdk.nashorn.internal.parser.JSONParser;

import java.io.*;
import java.util.List;

public class write {
    public static void main(String[] args) throws IOException {

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("D:\\code\\file\\cp_info.json"), "UTF-8"));
//
        String line ;

        String data = "";

        while ((line=reader.readLine())!= null){

            data += line;

        }

        reader.close();

//        System.out.println(data);

        JSONObject parse = (JSONObject)JSONObject.parse(data);
        JSONArray jsonArray = parse.getJSONArray("data");
        for (Object o : jsonArray) {
            JSONObject jsonObject = (JSONObject) o;
            String c0 = jsonObject.getString("_c0");
            String cp_play_mode = jsonObject.getString("cp_play_mode");
//            System.out.println("SELECT * from d3.cp WHERE cp.cp_play_mode = "+cp_play_mode+ "';");

            System.out.println("WHEN cp.cp_play_mode like '%"+ StrUtil.subAfter(cp_play_mode,'$',true) + "%'  THEN  '" +c0+"'" );
        }

    }


    class anylise{
        public List<entry> data;

        class entry{
            String _c0;
            String cp_play_mode;

            public entry(String _c0, String cp_play_mode) {
                this._c0 = _c0;
                this.cp_play_mode = cp_play_mode;
            }
        }

        public anylise(List<entry> data) {
            this.data = data;
        }
    }
}
