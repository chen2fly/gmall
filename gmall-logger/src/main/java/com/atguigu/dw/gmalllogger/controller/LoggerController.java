package com.atguigu.dw.gmalllogger.controller;

import com.alibaba.fastjson.JSONObject;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;

@Api(value = "LoggerController", description = "LoggerController")
//@Controller
@RestController   // 等价于: @Controller + @ResponseBody
public class LoggerController {


	@ApiOperation(value = "用户登录", notes = "用户登录接口")
	@ApiImplicitParams({
			@ApiImplicitParam(name = "username", value = "用户名", required = true, dataType = "string"),
			@ApiImplicitParam(name = "passwd", value = "密码", required = true, dataType = "string")
	})
	@RequestMapping(value = "/login", method = {RequestMethod.POST, RequestMethod.GET})
	@ResponseBody
	public String login(Users data, HttpServletRequest request) {
		return "ok";
	}


	//private final Logger logger = (Logger) LoggerFactory.getLogger(LoggerController.class);
	//    @RequestMapping(value = "/log", method = RequestMethod.POST)
	//    @ResponseBody  //表示返回值是一个 字符串, 而不是 页面名
	@PostMapping("/log")  // 等价于: @RequestMapping(value = "/log", method = RequestMethod.POST)
	public String doLog(@RequestParam("log") String log) {
		//JSONObject jsonObject1 = JSONObject.parseObject(log);
		//JSONObject jsonObject = addTS(jsonObject1);
		//logger.info(log);
		System.out.println(log);
		return "SUCCESS";
	}

	/**
	 * 业务:
	 *
	 * 1. 给日志添加时间戳 (客户端的时间有可能不准, 所以使用服务器端的时间)
	 *
	 * 2. 日志落盘
	 *
	 * 3. 日志发送 kafka
	 */
	/**
	 * 添加时间戳
	 *
	 * @param logObj
	 * @return
	 */
	public JSONObject addTS(JSONObject logObj) {
		logObj.put("ts", System.currentTimeMillis());
		return logObj;
	}


}
